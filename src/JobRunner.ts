import { DelayedError } from "bullmq";
import {
    ActiveJob,
    AttemptData,
    StepStateData
} from "./Job.ts";

export type StepOptions = {
    id: string;
};
export type RepeatOptions = {
    id: string;
    everyMs: number;
    limit: number;
    type: "fixed";
};
export type InvokeWaitForResultOptions = {
    id: string;
    errorOnFail: boolean;
};


export class RepeatHelper {
    public completed = false;

    constructor (public options: RepeatOptions) {
    }

    complete () {
        this.completed = true;
    }
}

export type StepType =
  "step"
  | "sleep"
  | "sleep-until"
  | "repeat"
  | "invoke-wait-for-result";
export type RunnerPurpose =
  "execute"
  | "analyze";

export type PrepareStepResult = {
    isCanRun: boolean;
    isCompleted: boolean;
    isExecutable: boolean;
    state: StepStateData | null;
    step: StepRunner;
};
export type ExecuteStepResult = {
    success: boolean;
    ran: boolean;
    prepared: PrepareStepResult;
    cachedResult?: unknown;
    executedResult?: ExecuteResult;
};
export type JobRunnerOptions = {
    purpose: RunnerPurpose;
    job: ActiveJob;
};

/**
 The JobRunner is the runtime API for while a job is running. Anything that needs to be done reliably should be done
 using JobRunner, as it guarantees execution.
 */
export class JobRunner {
    public purpose: RunnerPurpose;
    public job: ActiveJob;
    public ctx: RunnerContext;

    constructor (public options: JobRunnerOptions) {
        this.purpose = options.purpose;
        this.job = options.job;
        this.ctx = new RunnerContext(this);
    }

    /**
     * Declares the block of code as a durable step to be executed
     * @param options
     * @param handler
     */
    async step<
      F extends (() => Promise<any>),
      T extends Awaited<ReturnType<F>> = Awaited<ReturnType<F>>
    > (
      options: StepOptions,
      handler: F
    ): Promise<T> {
        const execution = await this.#executeStep({
            id: options.id,
            type: "step",
            handler,
            job: this.job,
            ctx: this.ctx,
            runner: this
        });

        if (!execution.success) {
            throw new Error(`Step ${ options.id } failed. Error: ${ execution.executedResult?.error }`);
        }

        const resultValue = (
          execution.cachedResult ?? execution.executedResult?.result
        );
        return resultValue as T;
    }

    /**
     * Delays the active job for the given duration.
     * @param options
     * @param duration the duration to delay the job for in milliseconds
     */
    async sleep (
      options: StepOptions,
      duration: number
    ): Promise<void> {
        const execution = await this.#executeStep({
            id: options.id,
            type: "sleep",
            duration,
            job: this.job,
            ctx: this.ctx,
            runner: this
        });

        if (execution.success && execution.ran) {
            if (!execution.executedResult?.sleep) {
                throw new Error(`Expected step ${ options.id } to have a sleep function.`);
            }

            return await execution.executedResult.sleep();
        } else if (execution.success && !execution.ran) {
            return;
        }

        throw new Error(`Sleep step ${ options.id } failed.`);
    }

    /**
     * Delays the active job until the given timestamp.
     * @param options
     * @param date
     */
    sleepUntil (
      options: StepOptions,
      date: Date
    ) {

    }

    /**
     * Repeats a durable block of code in a given interval, up to the specified limit, or until the repeat helper instance
     * is marked as complete.
     * @param options
     * @param handler
     */
    repeat (
      options: RepeatOptions,
      handler: (helper: RepeatHelper) => Promise<any>
    ) {

    }

    /**
     * Invokes another function and waits for the result before continuing.
     * @param options
     * @param action
     */
    invokeWaitForResult (
      options: InvokeWaitForResultOptions,
      action: any
    ) {

    }

    async #executeStep (options: RunnerStepOptions): Promise<ExecuteStepResult> {
        const prepared = this.#prepareStep(options);
        if (!prepared.isExecutable) {
            if (prepared.state?.status === "completed") {
                this.ctx.log.info(`Step ${ options.id } is already completed`);
                return {
                    success: true,
                    ran: false,
                    prepared,
                    cachedResult: prepared.state.result ?? null
                };
            }
            throw new Error(`Step ${ options.id } is not runnable`);
        }

        const result = await prepared.step.execute()
          .catch(e => e);

        if (!result || result.status === "failed") {
            this.ctx.log.debug(
              `Step ${ options.id } has thrown an error.`,
              result
            );
            throw new Error(`Step ${ options.id } failed.`);
        } else if (result?.status !== "completed" && result.error instanceof Error) {
            throw result;
        }

        return {
            success: true,
            ran: true,
            prepared,
            executedResult: result
        };
    }

    #prepareStep (options: RunnerStepOptions): PrepareStepResult {
        const step = new StepRunner(options);
        const isCanRun = step.canRun();
        const isCompleted = step.getState()?.status === "completed";
        const state = step.getState();
        const isExecutable = isCanRun && !isCompleted;

        return {
            isCanRun,
            isCompleted,
            isExecutable,
            state,
            step
        };
    }
}


export type LogEntry = {
    level: "debug" | "info" | "warn" | "error";
    message: string;
    data?: any;
};

export class ContextLogger {
    public logs: LogEntry[] = [];

    constructor (public context: RunnerContext) {

    }

    log (
      level: "debug" | "info" | "warn" | "error",
      message: string,
      data?: any
    ) {
        // console.log(level, message, data);
        this.logs.push({
            level,
            message,
            data
        });
    }

    debug (
      message: string,
      data?: any
    ) {
        this.log(
          "debug",
          message,
          data
        );
    }

    info (
      message: string,
      data?: any
    ) {
        this.log(
          "info",
          message,
          data
        );
    }

    warn (
      message: string,
      data?: any
    ) {
        this.log(
          "warn",
          message,
          data
        );
    }

    error (
      message: string,
      data?: any
    ) {
        this.log(
          "error",
          message,
          data
        );
    }
}

export type AttemptEntry = AttemptData;

export type ContextAttemptOptions = {
    type: "job" | "step";
    name: string;
};

export class ContextAttempt {
    public data: AttemptEntry;

    constructor (
      public options: ContextAttemptOptions
    ) {
        this.data = {
            name: options.name,
            type: options.type,
            status: "active",
            start: Date.now(),
            end: null,
            error: null
        };
    }

    fail (error: any) {
        this.data.status = "failed";
        this.data.error = error;
    }

    complete () {
        this.data.status = "completed";
        this.data.end = Date.now();
    }

    format (): AttemptEntry {
        if (!this.data.end) {
            this.data.end = Date.now();
        }

        return {
            ...this.data,
            duration: (
              this.data?.end ?? 0
            ) - (
              this.data?.start ?? 0
            ),
            name: this.data.name,
        };
    }
}

export class RunnerContext {
    public log: ContextLogger;
    public jobAttempt: ContextAttempt;
    public stepAttempts: ContextAttempt[];
    public steps: StepRunner[];

    constructor (public job: any) {
        this.log = new ContextLogger(this);
        this.jobAttempt = new ContextAttempt({
            name: "job",
            type: "job"
        });
        this.stepAttempts = [];
        this.steps = [];
    }

    _stepAttempt (slug: string) {
        const attempt = new ContextAttempt({
            name: `step.${ slug }`,
            type: "step"
        });
        this.stepAttempts = [
            ...this.stepAttempts,
            attempt
        ];
        return attempt;
    }

    _registerStep (step: StepRunner) {
        this.steps.push(step);
        return this;
    }

    _formatAttempts () {
        return this.stepAttempts.map(a => a.format());
    }
}

export type StepHandlerAlternatives =
  {
      type: "step";
      handler: () => Promise<any>;
  }
  | {
    type: "sleep";
    duration: number;
}
  | {
    type: "sleep-until";
    timestamp: number;
}
  | {
    type: "repeat";
    interval: number;
    limit: number;
    handler: (helper: RepeatHelper) => Promise<any>;
}
  | {
    type: "invoke-wait-for-result";
    action: any;
}

export type RunnerStepOptions =
  {
      id: string;
      type: StepType;
      runner: JobRunner;
      ctx: RunnerContext;
      job: ActiveJob;
  }
  & StepHandlerAlternatives;

export type ExecuteResult = {
    status: "no-run" | "completed" | "failed";
    result: any | null;
    state: StepStateData;
    error?: Error;
    sleep?: () => Promise<any>;
};

export class StepRunner {
    public runner: JobRunner;
    public ctx: RunnerContext;
    public job: ActiveJob;
    public id: string;
    public type: StepType;

    constructor (public options: RunnerStepOptions) {
        const {
            id,
            type,
            runner,
            ctx,
            job
        } = options;
        this.id = id;
        this.type = type;
        this.runner = runner;
        this.ctx = ctx;
        this.job = job;
    }

    async execute (): Promise<ExecuteResult> {
        //> Preparing for execution
        const runnable = this.canRun();
        const firstState = this.getState();
        // Registering the step in the context
        this.ctx._registerStep(this);

        //> Pre-execution checks
        if (!runnable) {
            this.ctx.log.info(`Step ${ this.id } is not runnable`);
            throw new Error(`Step ${ this.id } is not runnable`);
        }

        if (firstState) {
            if (firstState.status === "completed") {
                this.ctx.log.info(`Step ${ this.id } is already completed`);
                return {
                    result: firstState.result,
                    status: "no-run",
                    state: firstState
                };
            }
        }

        if (!firstState) {
            this.ctx.log.info(`First time running step ${ this.id }, saving step to state`);
            await this.setState({
                slug: this.id,
                status: "pending",
                attempts: 0,
                data: null,
                error: null
            });
        }

        //> Executing
        const attempt = this.ctx._stepAttempt(this.id);
        const state = this.getState();

        if (!state) {
            throw new Error(`Step ${ this.id } has no state`);
        }

        if (this.type === "sleep") {
            state.status = "completed";
            state.result = null;
            await this.setState(state);

            //> Altering the priority of the job so that when it's re-added, it will be done asap
            const bj = this.job.getBullJob();
            await bj.changePriority({
                priority: this.job.getDelayedPriority()
            });

            return {
                status: "completed",
                result: null,
                state,
                sleep: this.#runSleep
            };
        }

        if (this.type === "step") {
            let result: any | null = null;
            try {
                result = await this.#runStep();
                attempt.complete();
                state.status = "completed";
                this.ctx.log.debug(`Step ${ this.id } finished`);
            }
            catch (e: any) {
                this.ctx.log.debug(
                  `Step ${ this.id } has thrown an error.`,
                  e
                );
                state.status = "failed";
                state.error = e;
                attempt.fail(e);
            }

            state.result = result;
            await this.setState(state);

            if (state.status !== "completed") {
                return {
                    status: "failed",
                    result,
                    state
                };
            }

            return {
                status: "completed",
                result,
                state
            };
        }

        throw new Error(`Unknown step type ${ this.type }`);
    }

    canRun () {
        return this.runner.purpose === "execute";
    }

    /**
     * Gets the state of this step only
     */
    getState (): StepStateData | null {
        const data = this.job.getParsedData();
        return data.__state[this.id] || null;
    }

    /**
     *
     * @param state
     */
    async setState (state: StepStateData) {
        const data = this.job.getParsedData();
        data.__state[this.id] = state;
        await this.job.updateData(data);
    }

    #runStep = () => {
        if (this.options.type !== "step" || !this.options.handler) {
            throw new Error(`Missing handler or type for #runStep()`);
        }

        return this.options.handler();
    };

    #runSleep = async () => {
        if (this.options.type !== "sleep" || !this.options.duration) {
            throw new Error(`Missing duration or type for #runSleep()`);
        }

        const bullJob = this.job.getBullJob();
        const token = this.job.getBullToken();
        const timestamp = Date.now() + this.options.duration;

        this.ctx.log.info(`Sleeping for ${ this.options.duration }ms. Token: ${ token }`);
        await bullJob.moveToDelayed(
          timestamp,
          token
        );

        throw new DelayedError();
    };

    async #runSleepUntil () {
        if (this.options.type !== "sleep-until" || !this.options.timestamp) {
            throw new Error(`Missing timestamp or type for #runSleepUntil()`);
        }

        const bullJob = this.job.getBullJob();
        const token = this.job.getBullToken();
        const timestamp = this.options.timestamp;

        this.ctx.log.info(`Sleeping until ${ timestamp }. Token: ${ token }`);
        await bullJob.moveToDelayed(
          timestamp,
          token
        );

        throw new DelayedError();
    }

    async #runRepeat () {

    }

    async #runInvokeWaitForResult () {
    }
}
