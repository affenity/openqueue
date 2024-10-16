# OpenQueue

A highly opinionated, highly simplistic, highly not recommended for anyone else to use, library that looks
like inngest.com and trigger.dev, but is free, straightforward to use, and are built on top of existing tools (
BullMQ/Redis).

This project is meant more like a proof of concept, and is not a production ready library. Most likely you should not
even use
it for your hobby projects.

## Installing

```bash
bun add openqueue
```

## Usage

```typescript
//> Declaring an "Action" (Queue & Worker)
const processVideo = OpenQueue.action(
  {
      slug: "process-video",
      schema: z.object({
          videoUrl: z.string()
      })
  },
  {
      jobs: {
          retries: {
              max: 3,
              delay: 1000,
              backoff: "fixed"
          }
      }
  },
  async ({
      ctx,
      job,
      runner,
      data
  }) => {
      const downloadVideo = await runner.step(
        { id: "download-video" },
        () => downloadVideoFromUrl(data.videoUrl)
      );

      const transcodeVideo = await runner.step(
        { id: "transcode-video" },
        () => transcodeVideo(downloadVideo)
      );

      const generateTranscripts = await runner.step(
        { id: "generate-transcripts" },
        () => generateTranscripts(transcodeVideo)
      );

      const writeToDb = await runner.step(
        { id: "write-to-db" },
        () => db.video.create({
            downloadedId: downloadVideo.id,
            transcriptsLocation: generateTranscripts.location,
            transcodedVideoLocation: transcodeVideo.location
        })
      );

      return {
          dbId: writeToDb.id
      };
  }
);

//> Initiating
const client = OpenQueue.client({ redisUrl: "redis://" });
await client.init(); //> This prepared the bullmq queues and workers
await client.start(); //> This starts them for work

//> Adding a job
processVideo.addJob({
    videoUrl: "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
});

//> Adding a job with a unique id
processVideo.addJob(
  {
      videoUrl: "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
  },
  {
      uniqueJobId: "dQw4w9WgXcQ" // same as the video id
  }
);
```

As you can see, each step is run redundantly, and if any step fails, the previous successful steps won't be run again,
and the job will start from the failed step.

## How does it work?

It's a basic library with a few hundred lines of code, built on top of [BullMQ](https://bullmq.io) and Redis.
OpenQueue is meant as a simple layer on top to make the developer experience better, have per-step retry functionality,
and more.

When a jop is added and a worker is starting to work on it, OpenQueue will update the job data with a custom format, and
mark steps' status/result/error as the flow continues. If any step fails, it can be retried by retrying the entire job,
and OpenQueue
will know which steps are completed, and only execute the failed/pending steps.

For now, it's only a proof of concept and I'm simply testing it out!

## Future

I'm pretty sure this can be expanded on, adding more features. Some ideas I've had are:

- different steps (sleep, wait for event, wait for other job to complete, repeatable steps)
- different execution, in addition to publishing events which queues can subscribe to
- portal that is easy to self-host, simply `openqueue panel start` and full overview of queues, jobs and analytics
- integrate jobs into databases like Postgres. that way it's easy to move finished jobs to database for improved
  querying and to keep track of historical jobs
- much better DX