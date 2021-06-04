const cron = require('cron')
const moment = require('moment')
const Parse = require('parse/node')
const rp = require('request-promise')

const CronJob = cron.CronJob
const PARSE_TIMEZONE = 'UTC'

let cronJobs = {}

module.exports = (Parse) => {

    /**
     * Parse job schedule object
     * @typedef {Object} _JobSchedule
     * @property {String} id The job id
     */

    /**
     * Recreate the cron schedules for a specific _JobSchedule or all _JobSchedule objects
     * @param {_JobSchedule | string} [job=null] The job schedule to recreate. If not specified, all jobs schedules will be recreated.
     * Can be a _JobSchedule object or the id of a _JobSchedule object.
     */
    const recreateSchedule = async (job) => {
    if (job) {
        if (job instanceof String || typeof job === 'string') {
        try {
            const jobObject = await Parse.Object.extend('_JobSchedule').createWithoutData(job).fetch({
            useMasterKey: true
            })
            if (jobObject) {
            recreateJobSchedule(jobObject)
            } else {
            throw new Error(`No _JobSchedule was found with id ${job}`)
            }
        } catch (error) {
            throw error
        }
        } else if (job instanceof Parse.Object && job.className === '_JobSchedule') {
        recreateJobSchedule(job)
        } else {
        throw new Error('Invalid job type. Must be a string or a _JobSchedule')
        }
    } else {
        try {
        recreateScheduleForAllJobs()
        } catch (error) {
        throw error
        }
    }
    }

    /**
     * (Re)creates all schedules (crons) for all _JobSchedule from the Parse server
     */
    const recreateScheduleForAllJobs = async () => {
    if (!Parse.applicationId) {
        throw new Error('Parse is not initialized')
    }

    try {
        const results = await new Parse.Query('_JobSchedule').find({
        useMasterKey: true
        })

        destroySchedules()

        for (let job of results) {
        try {
            recreateJobSchedule(job)
        } catch (error) {
            console.log(error)
        }
        }
        console.log(`${Object.keys(cronJobs).length} job(s) scheduled.`)
    } catch (error) {
        throw error
    }
    }

    /**
     * (Re)creates the schedule (crons) of a _JobSchedule
     * @param {_JobSchedule} job The _JobSchedule
     */
    const recreateJobSchedule = (job) => {
    destroySchedule(job.id)
    cronJobs[job.id] = createCronJobs(job)
    }

    /**
     * Stop all jobs and remove them from the list of jobs
     */
    const destroySchedules = () => {
    for (let key of Object.keys(cronJobs)) {
        destroySchedule(key)
    }
    cronJobs = {}
    }

    /**
     * Destroy a planned cron job
     * @param {String} id The _JobSchedule id
     */
    const destroySchedule = (id) => {
    const jobs = cronJobs[id]
    if (jobs) {
        for (let job of jobs) {
        job.stop()
        }
        delete cronJobs[id]
    }
    }

    const createCronJobs = (job) => {
    const startDate = new Date(job.get('startAfter'))
    const repeatMinutes = job.get('repeatMinutes')
    const jobName = job.get('jobName')
    const params = job.get('params')
    const now = moment()

    // Launch just once
    if (!repeatMinutes) {
        return [
        new CronJob(
            startDate,
            () => { // On tick
            performJob(jobName, params)
            },
            null, // On complete
            true, // Start
            PARSE_TIMEZONE // Timezone
        )
        ]
    }
    // Periodic job. Create a cron to launch the periodic job a the start date.
    let timeOfDay = moment(job.get('timeOfDay'), 'HH:mm:ss.Z').utc()
    const daysOfWeek = job.get('daysOfWeek')
    const cronDoW = (daysOfWeek) ? daysOfWeekToCronString(daysOfWeek) : '*'
    const minutes = repeatMinutes % 60
    const hours = Math.floor(repeatMinutes / 60)

    let cron = '0 '
    // Minutes
    if (minutes) {
        cron += `${timeOfDay.minutes()}-59/${minutes} `
    } else {
        cron += `0 `
    }

    // Hours
    cron += `${timeOfDay.hours()}-23`
    if (hours) {
        cron += `/${hours}`
    }
    cron += ' '

    // Day of month
    cron += '* '

    // Month
    cron += '* '

    // Days of week
    cron += cronDoW

    console.log(`${jobName}: ${cron}`)

    const actualJob = new CronJob(
        cron,
        () => { // On tick
        performJob(jobName, params)
        },
        null, // On complete
        false, // Start
        PARSE_TIMEZONE // Timezone
    )

    // If startDate is before now, start the cron now
    if (moment(startDate).isBefore(now)) {
        actualJob.start()
        return [actualJob]
    }

    // Otherwise, schedule a cron that is going to launch our actual cron at the time of the day
    const startCron = new CronJob(
        startDate,
        () => { // On tick
        console.log('Start the cron')
        actualJob.start()
        },
        null, // On complete
        true, // Start
        PARSE_TIMEZONE // Timezone
    )

    return [startCron, actualJob]
    }

    /**
     * Converts the Parse scheduler days of week
     * @param {Array} daysOfWeek An array of seven elements for the days of the week. 1 to schedule the task for the day, otherwise 0.
     */
    const daysOfWeekToCronString = (daysOfWeek) => {
    const daysNumbers = []
    for (let i = 0; i < daysOfWeek.length; i++) {
        if (daysOfWeek[i]) {
        daysNumbers.push((i + 1) % 7)
        }
    }
    return daysNumbers.join(',')
    }

    /**
     * Perform a background job
     * @param {String} jobName The job name on Parse Server
     * @param {Object=} params The parameters to pass to the request
     */
    const performJob = async (jobName, params) => {
    try {
        const request = rp({
        method: 'POST',
        uri: Parse.serverURL + '/jobs/' + jobName,
        headers: {
            'X-Parse-Application-Id': Parse.applicationId,
            'X-Parse-Master-Key': Parse.masterKey
        },
        json: true // Automatically parses the JSON string in the response
        })
        if (params) {
        request.body = params
        }
        console.log(`Job ${jobName} launched.`)
    } catch (error) {
        console.log(error)
    }
    }

    const init = () => {

        // Recreates all crons when the server is launched
        recreateSchedule()

        // Recreates schedule when a job schedule has changed
        Parse.Cloud.afterSave('_JobSchedule', async (request) => {
            recreateSchedule(request.object)
        })

        // Destroy schedule for removed job
        Parse.Cloud.afterDelete('_JobSchedule', async (request) => {
            destroySchedule(request.object.id)
        })
    }

    init()
    
}

