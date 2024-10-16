import { Job as BullJob } from "bullmq";
import { z } from "zod";
import { OpenQueueClient } from "./Client.ts";
import { ActionDispatcher } from "./Dispatcher.ts";


export const StatusSchema = z.enum([
    "pending",
    "active",
    "completed",
    "failed"
]);
export const StepStateSchema = z.object({
    slug: z.string(),
    status: StatusSchema.default("pending"),
    attempts: z.number()
      .default(0),
    data: z.any()
      .nullish(),
    error: z.any()
      .nullish(),
    result: z.any()
      .nullish()
});
export const AttemptSchema = z.object({
    type: z.enum([
        "job",
        "step"
    ]),
    status: z.enum([
        "active",
        "failed",
        "completed"
    ]),
    name: z.string(),
    start: z.number(),
    end: z.number()
      .nullish(),
    duration: z.number()
      .nullish(),
    error: z.any()
      .nullish()
});
export const ActiveJobSchema = z.object({
    source: z.any(),
    __prepared: z.boolean()
      .default(false),
    __state: z.record(
        z.string(),
        StepStateSchema
      )
      .default({}),
    __attempts: z.array(AttemptSchema)
      .default([]),
    __step_attempts: z.array(AttemptSchema)
      .default([])
});

export type StatusData = z.infer<typeof StatusSchema>;
export type StepStateData = z.infer<typeof StepStateSchema>;
export type AttemptData = z.infer<typeof AttemptSchema>;
export type AttemptDataInput = z.input<typeof AttemptSchema>;
export type ActiveJobData<T> =
  z.infer<typeof ActiveJobSchema>
  & {
    source: T;
};
export type ActiveJobOptions = {
    bullJob: BullJob;
    bullToken?: string;
};

export class ActiveJob<T = any> {
    readonly #bullJob: BullJob;
    readonly #bullToken?: string;

    constructor (
      public client: OpenQueueClient,
      public dispatcher: ActionDispatcher<any, T>,
      public options: ActiveJobOptions
    ) {
        this.#bullJob = options.bullJob;
        this.#bullToken = options.bullToken;

        this.prepare();
    }

    /**
     * This function is called when ActiveJob is constructed. It checks if the job data has already been prepared, and if not
     * formats it correctly and marks it as prepared for next time, if any.
     */
    prepare (): ActiveJobData<T> {
        const original = this.#bullJob.data;
        const parsedOriginal = this.parse(original);

        if (parsedOriginal.__prepared) {
            return parsedOriginal as ActiveJobData<T>;
        }

        const newData = this.parse({
            source: original,
            __prepared: true
        });
        this.#bullJob.data = newData;

        return newData as ActiveJobData<T>;
    }

    getSourceData (): T {
        const source = this.getParsedData().source;
        return this.parseSource(source);
    }

    getParsedData () {
        return this.parse(this.#bullJob.data);
    }

    updateData (input: z.input<typeof ActiveJobSchema>) {
        const parsedUpdatedData = this.parse(input);
        return this.#bullJob.updateData(parsedUpdatedData);
    }

    getBullToken () {
        return this.#bullToken;
    }

    getBullJob () {
        return this.#bullJob;
    }

    getDefaultPriority () {
        return this.dispatcher.config.jobs?.priority?.defaultValue ?? 2;
    }

    getDelayedPriority () {
        return this.dispatcher.config.jobs?.priority?.delayDefaultValue ?? 1;
    }

    parse (input: z.input<typeof ActiveJobSchema>) {
        return ActiveJobSchema.parse(input);
    }

    parseSource (input: any): T {
        return this.dispatcher.schema.parse(input) as T;
    }
}
