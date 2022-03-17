import { Client, ClientConfig } from 'hazelcast-client';
import { HazelcastClient } from 'hazelcast-client/lib/HazelcastClient';
import { Duration, KeyedObject, ZBClient } from 'zeebe-node';
import { ProcessOutcome } from './WebSocketAPI';

export class ServiceAffinity {
  static hazelcastClient: HazelcastClient;
  static zeebeClient: ZBClient;

  affinityCallbacks: {
    // eslint-disable-next-line no-unused-vars
    [processInstanceKey: string]: (processOutcome: ProcessOutcome) => void;
  };

  async init(gatewayAddress: string, hazelcastOptions: ClientConfig) {
    ServiceAffinity.hazelcastClient = await Client.newHazelcastClient(
      hazelcastOptions,
    );
    ServiceAffinity.zeebeClient = new ZBClient(gatewayAddress);
  }

  async createAffinityWorker(taskType: string): Promise<void> {
    // create worker (ZB client)
    ServiceAffinity.zeebeClient.createWorker({
      taskType,
      taskHandler: async (job) => {
        try {
          console.log(`Publishing on topic ${job.processInstanceKey} started`);
          const updatedVars = {
            ...job?.variables,
            processInstanceKey: job?.processInstanceKey,
          };
          const topic = await ServiceAffinity.hazelcastClient.getReliableTopic(
            job.processInstanceKey,
          );
          await topic.publish(updatedVars);
          console.log(
            `Publishing on topic ${job.processInstanceKey} is complete.`,
          );
          return await job.complete(updatedVars);
        } catch (error: any) {
          console.error(
            `Error while publishing message on channel: ${job.processInstanceKey}`,
          );
          return await job.fail(error.message);
        }
      },
    });
  }

  async createProcessInstanceWithAffinity<Variables = KeyedObject>({
    bpmnProcessId,
    variables,
    cb,
  }: {
    bpmnProcessId: string;
    variables: Variables;
    cb: (processOutcome: ProcessOutcome) => void;
  }): Promise<void> {
    try {
      // create process instance (ZB client)
      const { processInstanceKey } =
        await ServiceAffinity.zeebeClient.createProcessInstance(
          bpmnProcessId,
          variables,
        );
      const topic = await ServiceAffinity.hazelcastClient.getReliableTopic(
        processInstanceKey,
      );

      this.affinityCallbacks[processInstanceKey] = cb;

      topic.addMessageListener(async (message: any) => {
        console.log(`Executing callback for topic ${processInstanceKey}`);
        try {
          await this.affinityCallbacks[processInstanceKey](message);
        } catch (err) {
          console.error(err);
        }
      });
    } catch (err) {
      console.error(err);
      throw err;
    }
  }

  async publishMessageWithAffinity<Variables = KeyedObject>({
    correlationKey,
    messageId,
    name,
    variables,
    processInstanceKey,
    cb,
  }: {
    correlationKey: string;
    messageId: string;
    name: string;
    variables: Variables;
    processInstanceKey: string;
    cb: (processOutcome: ProcessOutcome) => void;
  }): Promise<void> {
    await ServiceAffinity.zeebeClient.publishMessage({
      correlationKey,
      messageId,
      name,
      variables,
      timeToLive: Duration.minutes.of(10),
    });

    const topic = await ServiceAffinity.hazelcastClient.getReliableTopic(
      processInstanceKey,
    );

    this.affinityCallbacks[processInstanceKey] = cb;

    topic.addMessageListener(async (message: any) => {
      console.log(`Executing callback for topic ${processInstanceKey}`);
      try {
        await this.affinityCallbacks[processInstanceKey](message);
      } catch (err) {
        console.error(err);
      }
    });
  }

  async cleanup(processInstanceKey: string): Promise<void> {
    console.log(
      `Unsubscribe from topic ${processInstanceKey} and removing affinity callbacks.`,
    );

    const topic = await ServiceAffinity.hazelcastClient.getReliableTopic(
      processInstanceKey,
    );

    try {
      await topic.destroy();
    } catch (err) {
      console.error(err);
    }
  }
}
