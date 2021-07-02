/* eslint-disable no-shadow */
import _ from 'lodash';
import { Logger } from 'winston';
import * as axios from 'axios';
import * as fs from 'fs';
import { CmrGranule } from 'util/cmr';
import CmrStacCatalog from 'app/query-cmr/app/stac/cmr-catalog';
import Catalog from 'app/query-cmr/app/stac/catalog';
import path = require('path');
import logger from 'util/log';
import BaseService, { functionalSerializeOperation } from './base-service';
import InvocationResult from './invocation-result';
import { batchOperations } from '../../util/batch';

import env = require('../../util/env');

export interface ArgoServiceParams {
  argo_url: string;
  namespace: string;
  template: string;
  template_type?: string;
  template_ref?: string;
  embedded_template?: string;
  image_pull_policy?: string;
  cmr_granule_locator_image_pull_policy?: string;
  parallelism?: number;
  postBatchStepCount?: number;
  env: { [key: string]: string };
  image?: string;
}

interface ArgoVariable {
  name: string;
  value?: string;
  valueFrom?: {
    secretKeyRef?: {
      name: string;
      key: string;
    };
  };
}

/**
 * Hackfest - construct STAC catalog for a single granule
 * @param collection - the CMR collection
 * @param granule - the CMR granule
 * @returns the stac catalog
 */
function constructSingleGranuleCatalog(collection: string, granule: CmrGranule): Catalog {
  const catalog = new CmrStacCatalog({ description: `Catalog for ${collection} and ${granule.id}` });
  catalog.links.push({
    rel: 'harmony_source',
    href: `${process.env.CMR_ENDPOINT}/search/concepts/${collection}`,
  });
  catalog.addCmrGranules([granule], '');
  return catalog;
}

const _cannedResponse = {
  batch_completed: 'true',
  batch_count: 1,
  post_batch_step_count: 0,
  items: [{
    temporal: '2020-01-01T00:00:00.000Z,2020-01-01T01:59:59.000Z',
    bbox: [
      -180,
      -90,
      180,
      90,
    ],
    href: 's3://local-staging-bucket/public/harmony/service-example/824c037e-9c54-4894-9e26-29310f7313cd/001_00_7f00ff_global_blue_var_regridded_subsetted.nc.png',
    type: 'image/png',
    title: '001_00_7f00ff_global_blue_var_regridded_subsetted.nc.png',
    roles: [
      'data',
    ],
  }],
};

/**
 * Reads the STAC catalog and converts to a response body to send to the harmony backend to add
 * links to a job.
 * @param catalogFilename - the name of the catalog file
 * @returns the response to send
 */
async function generateCallbackBody(catalogFilename: string): Promise<object> {
  const body = {
    batch_completed: 'true',
    batch_count: 1,
    post_batch_step_count: 0,
    items: [],
  };
  // Read the catalog
  const catalog = JSON.parse(fs.readFileSync(catalogFilename).toString());
  const catalogRoot = path.dirname(catalogFilename);
  const relativeGranuleLink = catalog.links[2].href;
  const granuleLink = `${catalogRoot}/${relativeGranuleLink.replace(/^\./, '')}`;
  logger.info(`RelativeGranuleLink: ${relativeGranuleLink}, granuleLink: ${granuleLink}, catalogRoot: ${catalogRoot}`);
  const item = JSON.parse(fs.readFileSync(granuleLink).toString());
  const temporal = `${item.properties.start_datetime},${item.properties.end_datetime}`;
  const { bbox } = item;
  const { href, type, title, roles } = item.assets.data;
  const granule = { temporal, bbox, href, type, title, roles };

  body.items.push(granule);

  return body;
}

/**
 * Service implementation which invokes an Argo workflow and creates a Job to poll for service
 * updates.
 */
export default class ArgoService extends BaseService<ArgoServiceParams> {
  /**
   * Returns the batch size to use for the given request
   *
   * @param maxGranules - The system-wide maximum granules
   * @returns The number of granules per batch of results processed
   */
  chooseBatchSize(maxGranules = env.maxGranuleLimit): number {
    const maxResults = this.operation.maxResults || Number.MAX_SAFE_INTEGER;
    let batchSize = _.get(this.config, 'batch_size', env.defaultBatchSize);
    batchSize = batchSize <= 0 ? Number.MAX_SAFE_INTEGER : batchSize;

    return Math.min(maxGranules, batchSize, maxResults);
  }

  /**
   * Returns the page size to use for the given request
   *
   * @param maxGranules - The system-wide maximum granules
   * @returns The number of granules per page of results from the CMR
   */
  choosePageSize(maxGranules = env.maxGranuleLimit): number {
    const maxResults = this.operation.maxResults || Number.MAX_SAFE_INTEGER;

    return Math.min(maxResults, maxGranules, env.cmrMaxPageSize);
  }

  /**
   * Invokes an Argo workflow to execute a service request
   * @param logger - the logger
   * @returns A promise resolving to null
   */
  async _runArgoWorkflow(logger: Logger): Promise<void> {
    const url = `${this.params.argo_url}/api/v1/workflows/${this.params.namespace}`;

    const goodVars = _.reject(Object.keys(this.params.env),
      (variable) => _.includes(['OAUTH_UID', 'OAUTH_PASSWORD', 'EDL_USERNAME', 'EDL_PASSWORD'], variable));
    const dockerEnv = _.map(goodVars,
      (variable) => ({ name: variable, value: this.params.env[variable] }));

    // similarly we need to get at the model for the operation to retrieve parameters needed to
    // construct the workflow
    const serializedOperation = this.serializeOperation();
    const operation = JSON.parse(serializedOperation);

    let params = [
      {
        name: 'callback',
        value: operation.callback,
      },
      // Only needed for legacy workflow templates
      {
        name: 'image',
        value: this.params.image,
      },
      {
        name: 'image-pull-policy',
        value: this.params.image_pull_policy || env.defaultImagePullPolicy,
      },
      {
        name: 'cmr-granule-locator-image-pull-policy',
        value: env.cmrGranuleLocatorImagePullPolicy || env.defaultImagePullPolicy,
      },
      {
        name: 'timeout',
        value: `${env.defaultArgoPodTimeoutSecs}`, // Could use request specific value in the future
      },
      {
        name: 'post-batch-step-count',
        value: `${this.params.postBatchStepCount || 0}`,
      },
      {
        name: 'page-size',
        value: `${this.choosePageSize()}`,
      },
      {
        name: 'batch-size',
        value: `${this.chooseBatchSize()}`,
      },
      {
        name: 'parallelism',
        value: this.params.parallelism || env.defaultParallelism,
      },
      {
        name: 'query',
        value: this.operation.cmrQueryLocations.join(' '),
      },
    ];

    params = params.concat(dockerEnv);

    const templateType = this.params.template_type || 'legacy';
    const body = templateType === 'chaining' ? this._chainedWorkflowBody(params) : this._legacyWorkflowBody(params);
    const startTime = new Date().getTime();
    logger.info('timing.workflow-submission.start');
    try {
      await axios.default.post(url, body);
    } catch (e) {
      logger.error(`Argo workflow creation failed: ${JSON.stringify(e.response?.data)}`);
      logger.error(`Argo url: ${url}`);
      logger.error(`Workflow body: ${JSON.stringify(body)}`);
      throw e;
    }
    const endTime = new Date().getTime();
    const workflowSubmitTime = endTime - startTime;
    const frontendSubmitTime = endTime - this.operation.requestStartTime.getTime();
    logger.info('timing.workflow-submission.end', { durationMs: workflowSubmitTime });
    logger.info('timing.frontend-request.end', { durationMs: frontendSubmitTime });
  }

  /**
   * Executes the request synchronously or invokes an Argo workflow to execute a service request
   *
   *  @param logger - the logger associated with the request
   *  @returns A promise resolving to null
   */
  async _runSync(logger: Logger): Promise<void> {
    logger.info('Running synchronously');
    // Construct a STAC catalog for the granule
    const catalog = constructSingleGranuleCatalog(
      this.operation.sources[0].collection, this.operation.syncGranule,
    );
    // logger.info(`The catalog: ${JSON.stringify(catalog)}`);
    const catalogDir = `${env.catalogDir}/${this.operation.requestId}`;
    const inputsDir = `${catalogDir}/inputs`;
    const outputsDir = `${catalogDir}/outputs`;
    logger.info(`Writing inputs to: ${inputsDir}`);
    logger.info(`Expecting outputs in: ${outputsDir}`);
    // Create the catalog subdirectory
    await fs.promises.mkdir(catalogDir);
    await fs.promises.mkdir(inputsDir);
    await fs.promises.mkdir(outputsDir);

    // Write the catalog
    const inputCatalog = `${inputsDir}/catalog0.json`;
    const outputCatalog = `${outputsDir}/catalog.json`;
    await catalog.write(inputCatalog, true);

    // Call the sync workflow handler (pass the operation and the location of the stac catalog
    // - no env vars needed)
    // Need to figure out which service to call here but can just hardcode for now to
    // harmony-service-example
    try {
      const serializedOp = functionalSerializeOperation(this.operation, this.config);
      const serializedOperation = JSON.parse(serializedOp);
      let port = 5000;
      if (this.params.template === 'asf-gdal-subsetter') {
        port = 5001;
      }

      // await axios.default.post('http://localhost:5000/work', serializedOperation);
      await axios.default.post(`http://localhost:${port}/work`, serializedOperation);
    } catch (e) {
      logger.warn('Call to service failed');
    }

    // assume /outputs has service catalog

    const harmonyBackend = `${this.operation.callback.replace('host.docker.internal', 'localhost')}`;
    logger.info(`Calling harmony: ${harmonyBackend}/argo-response`);

    // Temp test with the input catalog until the service is generating the outputCatalog
    const granuleCatalog = fs.existsSync(outputCatalog) ? outputCatalog : inputCatalog;

    const backendCallbackBody = await generateCallbackBody(granuleCatalog);
    logger.info(`Backend body: ${JSON.stringify(backendCallbackBody)}`);
    // await axios.default.post(`${harmonyBackend}/argo-response`, cannedResponse);
    await axios.default.post(`${harmonyBackend}/argo-response`, backendCallbackBody);
    // callback to say we're done with the job
    logger.info(`Calling harmony: ${harmonyBackend}/response to mark job as complete`);
    await axios.default.post(`${harmonyBackend}/response?status=successful&argo=true`);
    return null;
  }

  /**
   * Executes the request synchronously or invokes an Argo workflow to execute a service request
   *
   *  @param logger - the logger associated with the request
   *  @returns A promise resolving to null
   */
  async _run(logger: Logger): Promise<InvocationResult> {
    if (this.isSynchronous && this.operation.syncGranule) {
      this._runSync(logger);
    } else {
      this._runArgoWorkflow(logger);
    }

    return null;
  }

  _buildExitHandlerScript(): string {
    return `
    echo '{{workflow.failures}}' > /tmp/failures
    error="{{workflow.status}}"
    timeout_count=$(grep -c 'Pod was active on the node longer than the specified deadline' /tmp/failures)
    if [ "$timeout_count" != "0" ]
    then
    error="Request%20timed%20out"
    fi
    if [ "{{workflow.status}}" == "Succeeded" ]
    then
    curl -XPOST "{{inputs.parameters.callback}}/response?status=successful&argo=true"
    else
    curl -XPOST "{{inputs.parameters.callback}}/response?status=failed&argo=true&error=$error"
    fi
    `.trim();
  }

  /**
   * Returns a workflow POST body for Argo for invoking chainable services
   * @param params - The common workflow parameters to be passed to each service
   * @returns a JSON-serializable object to be POST-ed to initiate the Argo workflows
   */
  _chainedWorkflowBody(params: ArgoVariable[]): unknown {
    const { user, requestId } = this.operation;

    // we need to serialize the batch operation to get just the model and then deserialize
    // it so we can pass it to the Argo looping/concurrency mechanism in the workflow
    // which expects objects not strings
    const serializedOp = functionalSerializeOperation(this.operation, this.config);

    const serializedOperation = JSON.parse(serializedOp);
    for (const source of serializedOperation.sources) {
      delete source.granules;
    }

    const argoParams = [...params, { name: 'operation', value: JSON.stringify(serializedOperation) }];
    return {
      namespace: this.params.namespace,
      serverDryRun: false,
      workflow: {
        metadata: {
          generateName: `${this.params.template}-chain-`,
          namespace: this.params.namespace,
          labels: {
            user,
            request_id: requestId,
          },
        },
        spec: {
          arguments: {
            parameters: argoParams,
          },
          parallelism: this.params.parallelism || env.defaultParallelism,
          workflowTemplateRef: {
            name: `${this.params.template}-chain`,
          },
        },
      },
    };
  }

  /**
   * Returns a workflow POST body for Argo for invoking legacy (non-chained, low-granule limit)
   * services
   * @param params - The common workflow parameters to be passed to each service
   * @returns a JSON-serializable object to be POST-ed to initiate the Argo workflows
   */
  _legacyWorkflowBody(params: ArgoVariable[]): unknown {
    const { user, requestId } = this.operation;
    // Further limit the batch size so the POST body doesn't exceed Argo limits
    const batchSize = this.chooseBatchSize(Math.min(env.maxGranuleLimit, 200));
    const parallelism = this.params.parallelism || env.defaultParallelism;

    const batch = batchOperations(this.operation, batchSize);
    // we need to serialize the batch operations to get just the models and then deserialize
    // them so we can pass them to the Argo looping/concurrency mechanism in the workflow
    // which expects objects not strings
    const ops = batch.map((op) => JSON.parse(functionalSerializeOperation(op, this.config)));

    return {
      namespace: this.params.namespace,
      serverDryRun: false,
      workflow: {
        metadata: {
          generateName: `${this.params.template}-`,
          namespace: this.params.namespace,
          labels: {
            user,
            request_id: requestId,
          },
        },
        spec: {
          entryPoint: 'service',
          onExit: 'exit-handler',
          templates: [
            {
              name: 'service',
              parallelism,
              steps: [
                [
                  {
                    name: 'service',
                    templateRef: {
                      name: this.params.template,
                      template: this.params.template,
                    },
                    arguments: {
                      parameters: [
                        ...params,
                        { name: 'operation', value: '{{item}}' },
                        { name: 'batch-count', value: `${ops.length}` },
                      ],
                    },
                    withItems: ops,
                  },
                ],
              ],
            },
            {
              name: 'exit-handler',
              inputs: {
                parameters: params,
              },
              script: {
                image: 'curlimages/curl',
                imagePullPolicy: 'IfNotPresent',
                command: ['sh'],
                source: this._buildExitHandlerScript(),
              },
            },
          ],
        },
      },
    };
  }
}
