/*
 Dispatcher is a representation of the various choices for a function should be exuted.
 A dispatcher can be of type "action" (queue & worker), "cron" (a cron job) or "event" (event-triggered function).
 */

import {
    Job as BullJob,
    JobsOptions as BullJobsOptions,
    Queue as BullQueue,
    QueueOptions,
    Worker as BullWorker,
    WorkerOptions
} from "bullmq";
import { z } from "zod";
import { OpenQueueClient } from "./Client.ts";
import {
    ActiveJob,
    ActiveJobSchema
} from "./Job.ts";
import {
    JobRunner,
    RunnerContext
} from "./JobRunner.ts";

const AddJobOptionsSchema = z.object({
    uniqueJobId: z.string()
      .optional()
});


export type DispatcherType =
  "cron"
  | "event"
  | "action";
export type GenericDispatcherOptions = {
    slug: string;
    type: DispatcherType;
    schema: z.AnyZodObject;
};

export type ActionHandlerArgs<T> = {
    ctx: RunnerContext;
    runner: JobRunner;
    job: ActiveJob<T>;
    data: T;
};
export type ActionHandler<T> = (args: ActionHandlerArgs<T>) => Promise<any>;
export type ActionDispatcherOptions<T> =
  GenericDispatcherOptions
  & {
    handler: ActionHandler<T>;
};
export const ActionDispatcherConfigSchema = z.object({
      jobs: z.object({
            strategy: z.enum([
                  "fifo",
                  "lifo"
              ])
              .default("fifo"),
            concurrency: z.object({
                  enabled: z.boolean()
                    .default(true),
                  local: z.number()
                    .default(1),
                  global: z.number()
                    .optional()
              })
              .optional(),
            retries: z.object({
                  enabled: z.boolean()
                    .default(true),
                  max: z.number()
                    .default(3),
                  delay: z.number()
                    .default(1000),
                  backoff: z.enum([
                        "exponential",
                        "fixed"
                    ])
                    .default("exponential")
              })
              .optional(),
            rateLimit: z.object({
                  enabled: z.boolean()
                    .default(true),
                  amount: z.number(),
                  duration: z.number(),
                  onKey: z.string()
                    .nullish()
              })
              .optional(),
            debounce: z.object({
                  strategy: z.enum([
                        "fixed",
                        "extended"
                    ])
                    .default("fixed"),
                  enabled: z.boolean()
                    .default(true),
                  ttl: z.number(),
                  key: z.string()
              })
              .optional(),
            delay: z.object({
                  enabled: z.boolean()
                    .default(true),
                  strategy: z.enum([
                        "fixed",
                        "extended"
                    ])
                    .default("fixed"),
                  duration: z.number(),
                  key: z.string()
              })
              .optional(),
            repeat: z.object({
                  enabled: z.boolean()
                    .default(true),
                  every: z.number(),
                  limit: z.number(),
                  key: z.string()
                    .nullish()
              })
              .optional(),
            priority: z.object({
                  enabled: z.boolean(),
                  defaultValue: z.number()
                    .default(2),
                  /**
                   * A delayed job's new priority after it has been delayed. This is important to prevent it from behind
                   * all other jobs when re-added to the queue
                   */
                  delayDefaultValue: z.number()
                    .default(1)
              })
              .optional()
        })
        .default({}),
      removal: z.object({
            onComplete: z.object({
                  always: z.boolean()
                    .optional(),
                  age: z.number()
                    .optional(),
                  count: z.number()
                    .optional()
              })
              .optional(),
            onFail: z.object({
                  always: z.boolean()
                    .optional(),
                  age: z.number()
                    .optional(),
                  count: z.number()
                    .optional()
              })
              .optional()
        })
        .default({})
  })
  .default({});
export type ActionDispatcherConfig = z.infer<typeof ActionDispatcherConfigSchema>;
export type ActionDispatcherConfigInput = z.input<typeof ActionDispatcherConfigSchema>;

export class ActionDispatcher<S extends z.AnyZodObject, T extends z.infer<S>, O extends ActionDispatcherOptions<T> = ActionDispatcherOptions<T>, > {
    public client: OpenQueueClient | null = null;
    public bullQueue: BullQueue | null = null;
    public bullWorker: BullWorker | null = null;
    public handler: ActionHandler<T>;
    public schema: S;

    constructor (
      public options: O,
      public config: ActionDispatcherConfig
    ) {
        this.schema = options.schema as S;
        this.handler = options.handler;
    }

    processJob = async (
      bullJob: BullJob,
      bullToken?: string
    ) => {
        const activeJob = new ActiveJob<T>(
          this.client!,
          this as ActionDispatcher<S, T, O>,
          {
              bullJob,
              bullToken
          }
        );
        const runner = new JobRunner({
            job: activeJob,
            purpose: "execute"
        });

        const handlerResult = await this.handler({
            ctx: runner.ctx,
            runner,
            job: activeJob,
            data: activeJob.getSourceData()
        });

        try {
            const data = activeJob.getParsedData();
            data.__step_attempts = [
                ...data.__step_attempts,
                ...runner.ctx._formatAttempts()
            ];
            data.__attempts = [
                ...data.__attempts,
                runner.ctx.jobAttempt.format()
            ];
            await activeJob.updateData(data);
        }
        catch (e) {
            console.log(`ERROR. FAILED TO UPDATE STATE AFTER JOB RUN. ERROR: ${ e }`);
        }

        return handlerResult;
    };

    addJob (
      _input: z.input<S>,
      options: z.input<typeof AddJobOptionsSchema> = {}
    ) {
        const parsed = this.schema.parse(_input);
        const jobData = this.#formatNewJobData(parsed);

        return this.bullQueue!.add(
          "default",
          jobData,
          this.#formatJobOptions(options)
        );
    }

    addBulkJobs (_inputs: Array<{
        data: z.input<S>;
        options: z.input<typeof AddJobOptionsSchema>;
    }>) {
        const formattedJobs = _inputs.map(inp => (
          {
              name: "default",
              data: this.#formatNewJobData(this.schema.parse(inp.data)),
              options: this.#formatJobOptions(inp.options)
          }
        ));
        return this.bullQueue!.addBulk(formattedJobs);
    }

    async init (client: OpenQueueClient) {
        this.client = client;
        this.bullQueue = new BullQueue(
          this.options.slug,
          {
              ...this.#queueOptions(),
              connection: client.connection
          }
        );
        this.bullWorker = new BullWorker(
          this.options.slug,
          (
            job,
            token
          ) => this.processJob(
            job,
            token
          ),
          {
              ...this.#workerOptions(),
              connection: client.connection,
          }
        );

        if (this.config.jobs?.concurrency?.global) {
            await this.bullQueue.setGlobalConcurrency(this.config.jobs.concurrency.global);
        }
    }

    async startQueue () {
        await this.bullQueue!.resume();
    }

    async pauseQueue () {
        await this.bullQueue!.pause();
    }

    async start () {
        // We can't await the .run() as it freezes.
        this.bullWorker!.run();
        this.bullWorker!.resume();
    }

    async pause () {
        this.bullWorker!.pause();
    }

    #formatJobOptions (options: z.input<typeof AddJobOptionsSchema>): BullJobsOptions {
        const parsed = AddJobOptionsSchema.parse(options);
        return {
            //> job id for deduplication
            ...(
              parsed.uniqueJobId && { jobId: parsed.uniqueJobId }
            )
        };
    }

    #queueOptions () {
        const opts = {
            defaultJobOptions: {
                removeOnComplete: this.config.removal?.onComplete?.always ?? this.config.removal?.onComplete?.age
                  ?? this.config.removal?.onComplete?.count ?? false,
                removeOnFail: this.config.removal?.onFail?.always ?? this.config.removal?.onFail?.age
                  ?? this.config.removal?.onFail?.count ?? false,
                attempts: this.config.jobs?.retries?.enabled ? this.config.jobs?.retries?.max : 0, ...(
                  this.config.jobs?.retries?.enabled && {
                      backoff: {
                          type: this.config.jobs?.retries?.backoff,
                          delay: this.config.jobs?.retries?.delay
                      }
                  }
                ),
                delay: this.config.jobs?.delay?.enabled ? this.config.jobs?.delay?.duration : 0,
                lifo: this.config.jobs?.strategy === "lifo",
                priority: this.config.jobs?.priority?.enabled ? this.config.jobs?.priority?.defaultValue : 0
            }
        } satisfies Omit<QueueOptions, "connection">;

        return opts;
    }

    #workerOptions () {
        const opts = {
            // The user should explicitly start the worker
            autorun: false,
            concurrency: this.config.jobs?.concurrency?.enabled ? this.config.jobs?.concurrency?.local : 1, ...(
              this.config.jobs.rateLimit?.enabled && {
                  limiter: {
                      max: this.config.jobs.rateLimit.amount,
                      duration: this.config.jobs.rateLimit.duration
                  }
              }
            ), ...(
              this.config.jobs.debounce?.enabled && {
                  debounce: {
                      duration: this.config.jobs.debounce.ttl,
                      key: this.config.jobs.debounce.key
                  }
              }
            ), ...(
              this.config.jobs.delay?.enabled && {
                  delay: {
                      duration: this.config.jobs.delay.duration,
                      key: this.config.jobs.delay.key
                  }
              }
            ), ...(
              this.config.jobs.repeat?.enabled && {
                  repeat: {
                      every: this.config.jobs.repeat.every,
                      limit: this.config.jobs.repeat.limit,
                      key: this.config.jobs.repeat.key
                  }
              }
            ), ...(
              this.config.jobs.priority?.enabled && {
                  priority: {
                      default: this.config.jobs.priority.defaultValue
                  }
              }
            ), ...(
              this.config.jobs.retries?.enabled && {
                  attempts: this.config.jobs.retries.max,
                  backoff: this.config.jobs.retries.backoff,
                  delay: this.config.jobs.retries.delay
              }
            )
        } satisfies Omit<WorkerOptions, "connection">;

        return opts;
    }

    #formatNewJobData (data: z.infer<S>) {
        return ActiveJobSchema.parse({
            source: data,
            __prepared: true
        });
    }
}

export type CronDispatcherOptions =
  GenericDispatcherOptions
  & {};

export class CronDispatcher {
    constructor (options: CronDispatcherOptions) {

    }

}

export type EventDispatcherOptions =
  GenericDispatcherOptions
  & {};

export class EventDispatcher {
    constructor (options: EventDispatcherOptions) {

    }
}
