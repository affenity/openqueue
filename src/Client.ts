import Redis from "ioredis";
import { z } from "zod";
import {
    ActionDispatcher,
    ActionDispatcherConfigInput,
    ActionDispatcherConfigSchema,
    ActionDispatcherOptions,
    ActionHandler
} from "./Dispatcher.ts";

export type OpenQueueOptions = {
    redisUrl: string;
};

export type ActionOptions<S extends z.AnyZodObject> = {
    slug: string;
    schema: S;
};
export type ActionConfig = {};

export class OpenQueue {
    static action<
      Opts extends ActionOptions<z.AnyZodObject>,
      ConfInp extends ActionDispatcherConfigInput = ActionDispatcherConfigInput,
      Schema extends z.AnyZodObject = Opts["schema"],
      T extends z.infer<Schema> = z.infer<Schema>,
      Handler extends ActionHandler<T> = ActionHandler<T>,
      FullOptions extends ActionDispatcherOptions<T> = {
          slug: Opts["slug"];
          type: "action";
          schema: Opts["schema"];
          handler: Handler;
      }
    > (
      options: Opts,
      config: ConfInp,
      handler: Handler
    ) {
        const parsedConfig = ActionDispatcherConfigSchema.parse(config);
        const fullOptions = {
            slug: options.slug as Opts["slug"],
            type: "action",
            schema: options.schema as Opts["schema"],
            handler
        } satisfies ActionDispatcherOptions<T>;

        return new ActionDispatcher<Schema, T, FullOptions>(
          fullOptions as unknown as FullOptions,
          parsedConfig
        );
    }

    static client<Actions extends ActionsRecord> (
      options: OpenQueueOptions,
      actions: Actions
    ) {
        return new OpenQueueClient<Actions>(
          options,
          actions
        );
    }
}

export type ActionsRecord = Record<string, ActionDispatcher<z.AnyZodObject, any>>;

export class OpenQueueClient<
  Actions extends ActionsRecord = ActionsRecord
> {
    public connection: Redis;

    constructor (
      public options: OpenQueueOptions,
      public actions: Actions
    ) {
        const parsed = new URL(options.redisUrl);
        this.connection = new Redis({
            host: parsed.hostname,
            port: parseInt(parsed.port),
            password: parsed.password,
            username: parsed.username,
            maxRetriesPerRequest: null
        });
    }

    invoke<N extends keyof Actions> (
      name: N
    ) {

    }

    async init () {
        return Promise.all(
          Object.values(this.actions)
            .map(action => action.init(this))
        );
    }

    async start () {
        return Promise.all(
          Object.values(this.actions)
            .map(action => action.start())
        );
    }
}
