import { Response, NextFunction } from 'express';
import { sanitizeImage } from '../util/string';
import { validateJobId } from '../util/job';
import { Job, JobStatus, JobQuery } from '../models/job';
import { getWorkItemsByJobId } from '../models/work-item';
import { NotFoundError, RequestValidationError } from '../util/errors';
import { getPagingParams, getPagingLinks, setPagingHeaders } from '../util/pagination';
import HarmonyRequest from '../models/harmony-request';
import db from '../util/db';
import version from '../util/version';
import env = require('../util/env');
import { keysToLowerCase } from '../util/object';
import { WorkItemStatus } from '../models/work-item-interface';
import { getRequestRoot } from '../util/url';
import { getAllStateChangeLinks, getJobStateChangeLinks } from '../util/links';

/**
 * Maps job status to display class.
 */
const statusClass = {
  [JobStatus.ACCEPTED]: 'primary',
  [JobStatus.CANCELED]: 'secondary',
  [JobStatus.FAILED]: 'danger',
  [JobStatus.SUCCESSFUL]: 'success',
  [JobStatus.RUNNING]: 'info',
  [JobStatus.PAUSED]: 'warning',
  [JobStatus.PREVIEWING]: 'info',
};

/**
 * Return an object that contains key value entries for jobs table filters.
 * @param requestQuery - the Record given by keysToLowerCase
 * @param isAdminAccess - is the requesting user an admin
 * @param maxFilters - set a limit on the number of user requested filters
 * @returns object containing filter values
 */
function parseJobsFilter( /* eslint-disable @typescript-eslint/no-explicit-any */
  requestQuery: Record<string, any>,
  isAdminAccess: boolean,
  maxFilters = 30,
): {
    statusValues: string[], // need for querying db
    userValues: string[], // need for querying db
    originalValues: string[] // needed for populating filter input
  } {
  if (!requestQuery.jobsfilter) {
    return {
      statusValues: [],
      userValues: [],
      originalValues: [],
    };
  }
  const selectedOptions: { field: string, dbValue: string, value: string }[] = JSON.parse(requestQuery.jobsfilter);
  const validStatusSelections = selectedOptions
    .filter(option => option.field === 'status' && Object.values<string>(JobStatus).includes(option.dbValue));
  const statusValues = validStatusSelections.map(option => option.dbValue);
  const validUserSelections = selectedOptions
    .filter(option => isAdminAccess && /^user: [A-Za-z0-9\.\_]{4,30}$/.test(option.value));
  const userValues = validUserSelections.map(option => option.value.split('user: ')[1]);
  if ((statusValues.length + userValues.length) > maxFilters) {
    throw new RequestValidationError(`Maximum amount of filters (${maxFilters}) was exceeded.`);
  }
  const originalValues = validStatusSelections
    .concat(validUserSelections)
    .map(option => option.value);
  return {
    statusValues,
    userValues,
    originalValues,
  };
}

/**
 * Display jobs along with their status in the workflow UI.
 *
 * @param req - The request sent by the client
 * @param res - The response to send to the client
 * @param next - The next function in the call chain
 * @returns HTML page of clickable jobs which take the user to a
 * page where they can visualize the whole workflow as it happens
 */
export async function getJobs(
  req: HarmonyRequest, res: Response, next: NextFunction,
): Promise<void> {
  try {
    const requestQuery = keysToLowerCase(req.query);
    const jobQuery: JobQuery = { where: {}, whereIn: {} };
    if (!req.context.isAdminAccess) {
      jobQuery.where.username = req.user;
    }
    const disallowStatus = requestQuery.disallowstatus === 'on';
    const disallowUser = requestQuery.disallowuser === 'on';
    const jobsFilter = parseJobsFilter(requestQuery, req.context.isAdminAccess);
    if (jobsFilter.statusValues.length) {
      jobQuery.whereIn.status = {
        values: jobsFilter.statusValues,
        in: !disallowStatus,
      };
    }
    if (jobsFilter.userValues.length) {
      jobQuery.whereIn.username = {
        values: jobsFilter.userValues,
        in: !disallowUser,
      };
    }
    const { page, limit } = getPagingParams(req, env.defaultJobListPageSize, true);
    const { data: jobs, pagination } = await Job.queryAll(db, jobQuery, false, page, limit);
    setPagingHeaders(res, pagination);
    const pageLinks = getPagingLinks(req, pagination);
    const nextPage = pageLinks.find((l) => l.rel === 'next');
    const previousPage = pageLinks.find((l) => l.rel === 'prev');
    const currentPage = pageLinks.find((l) => l.rel === 'self');
    res.render('workflow-ui/jobs/index', {
      version,
      page,
      limit,
      currentUser: req.user,
      currentPage: currentPage.href,
      isAdminRoute: req.context.isAdminAccess,
      // job table row HTML
      jobs,
      jobBadge() {
        return statusClass[this.status];
      },
      jobCreatedAt() { return this.createdAt.getTime(); },
      jobUrl() {
        try {
          const url = new URL(this.request);
          const path = url.pathname + url.search;
          return path;
        } catch (e) {
          req.context.logger.error(`Could not form a valid URL from job.request: ${this.request}`);
          req.context.logger.error(e);
          return this.request;
        }
      },
      // job table filters HTML
      disallowStatusChecked: disallowStatus ? 'checked' : '',
      disallowUserChecked: disallowUser ? 'checked' : '',
      selectedFilters: jobsFilter.originalValues,
      // job table paging buttons HTML
      links: [
        { ...previousPage, linkTitle: 'previous' },
        { ...nextPage, linkTitle: 'next' },
      ],
      linkDisabled() { return (this.href ? '' : 'disabled'); },
      linkHref() { return (this.href || ''); },
    });
  } catch (e) {
    req.context.logger.error(e);
    next(e);
  }
}

/**
 * Display a job's progress and work items in the workflow UI.
 *
 * @param req - The request sent by the client
 * @param res - The response to send to the client
 * @param next - The next function in the call chain
 * @returns The workflow UI page where the user can visualize the job as it happens
 */
export async function getJob(
  req: HarmonyRequest, res: Response, next: NextFunction,
): Promise<void> {
  const { jobID } = req.params;
  try {
    validateJobId(jobID);
    const { page, limit } = getPagingParams(req, 1000);
    const job = await Job.byJobID(db, jobID, false);
    if (!job) {
      throw new NotFoundError(`Unable to find job ${jobID}`);
    }
    if (!(await job.canShareResultsWith(req.user, req.context.isAdminAccess, req.accessToken))) {
      throw new NotFoundError();
    }
    res.render('workflow-ui/job/index', {
      job,
      page,
      limit,
      version,
      isAdminRoute: req.context.isAdminAccess,
    });
  } catch (e) {
    req.context.logger.error(e);
    next(e);
  }
}

/**
 * Return job state change links so that the user can pause, resume, cancel, etc., a job.
 *
 * @param req - The request sent by the client
 * @param res - The response to send to the client
 * @param next - The next function in the call chain
 * @returns The job links (pause, resume, etc.)
 */
export async function getJobLinks(
  req: HarmonyRequest, res: Response, next: NextFunction,
): Promise<void> {
  const { jobID } = req.params;
  const { all } = req.query;
  try {
    validateJobId(jobID);
    const job = await Job.byJobID(db, jobID, false);
    if (!job) {
      throw new NotFoundError(`Unable to find job ${jobID}`);
    }
    if (!(await job.canShareResultsWith(req.user, req.context.isAdminAccess, req.accessToken))) {
      throw new NotFoundError();
    }
    if (!req.context.isAdminAccess && (job.username != req.user)) {
      // if the job is shareable but this non-admin user (req.user) does not own the job,
      // they won't be able to change the job's state via the state change links
      res.send([]);
      return;
    }
    const urlRoot = getRequestRoot(req);
    const links = all === 'true' ?
      getAllStateChangeLinks(job, urlRoot, req.context.isAdminAccess) :
      getJobStateChangeLinks(job, urlRoot, req.context.isAdminAccess);
    res.send(links);
  } catch (e) {
    req.context.logger.error(e);
    next(e);
  }
}

/**
 * Render the work items table for the workflow UI.
 *
 * @param req - The request sent by the client
 * @param res - The response to send to the client
 * @param next - The next function in the call chain
 * @returns The work items table HTML
 */
export async function getWorkItemsTable(
  req: HarmonyRequest, res: Response, next: NextFunction,
): Promise<void> {
  const { jobID } = req.params;
  const { checkJobStatus } = req.query;
  const badgeClasses = {};
  badgeClasses[WorkItemStatus.READY] = 'primary';
  badgeClasses[WorkItemStatus.CANCELED] = 'secondary';
  badgeClasses[WorkItemStatus.FAILED] = 'danger';
  badgeClasses[WorkItemStatus.SUCCESSFUL] = 'success';
  badgeClasses[WorkItemStatus.RUNNING] = 'info';
  try {
    validateJobId(jobID);
    const query: JobQuery = { where: { requestId: jobID } };
    if (!req.context.isAdminAccess) {
      query.where.username = req.user;
    }
    const { job } = await Job.byRequestId(db, jobID, 0, 0);
    if (job) {
      if (!(await job.canShareResultsWith(req.user, req.context.isAdminAccess, req.accessToken))) {
        throw new NotFoundError();
      }
      if (([JobStatus.SUCCESSFUL, JobStatus.CANCELED, JobStatus.FAILED].indexOf(job.status) > -1) && checkJobStatus === 'true') {
        // tell the client that the job has finished
        res.status(204).json({ status: job.status });
        return;
      }
      const { page, limit } = getPagingParams(req, env.defaultJobListPageSize);
      const { workItems, pagination } = await getWorkItemsByJobId(db, job.jobID, page, limit, 'asc');
      const pageLinks = getPagingLinks(req, pagination);
      const nextPage = pageLinks.find((l) => l.rel === 'next');
      const previousPage = pageLinks.find((l) => l.rel === 'prev');
      setPagingHeaders(res, pagination);
      res.render('workflow-ui/job/work-items-table', {
        job,
        statusClass: statusClass[job.status],
        workItems,
        workflowItemBadge() { return badgeClasses[this.status]; },
        workflowItemStep() { return sanitizeImage(this.serviceID); },
        workflowItemCreatedAt() { return this.createdAt.getTime(); },
        workflowItemUpdatedAt() { return this.updatedAt.getTime(); },
        links: [
          { ...previousPage, linkTitle: 'previous' },
          { ...nextPage, linkTitle: 'next' },
        ],
        linkDisabled() { return (this.href ? '' : 'disabled'); },
        linkHref() {
          return (this.href ? this.href
            .replace('/work-items', '')
            .replace(/(&|\?)checkJobStatus=(true|false)/, '') : '');
        },
      });
    } else {
      throw new NotFoundError(`Unable to find job ${jobID}`);
    }
  } catch (e) {
    req.context.logger.error(e);
    next(e);
  }
}
