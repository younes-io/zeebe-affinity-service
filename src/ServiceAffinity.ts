import { Client, ClientConfig } from 'hazelcast-client';
import { HazelcastClient } from 'hazelcast-client/lib/HazelcastClient';
import { Duration, KeyedObject, ZBClient } from 'zeebe-node';

export interface ProcessOutcome {
    processInstanceKey: string;
    variables: { [key: string]: string | number };
}

export class ServiceAffinity {
    private static hazelcastClient: HazelcastClient;
    private static zeebeClient: ZBClient;

    private affinityCallbacks: {
        [processInstanceKey: string]: (processOutcome: ProcessOutcome) => void;
    };

    private hazelcastListeners: {
        [processInstanceKey: string]: string;
    };

    private async init(gatewayAddress: string, hazelcastOptions: ClientConfig) {
        ServiceAffinity.hazelcastClient = await Client.newHazelcastClient(
            hazelcastOptions,
        );
        ServiceAffinity.zeebeClient = new ZBClient(gatewayAddress);
    }

    constructor(gatewayAddress: string, hazelcastOptions: ClientConfig) {
        this.hazelcastListeners = {};
        this.affinityCallbacks = {};
        this.init(gatewayAddress, hazelcastOptions)
            .then(() => {
                console.log(
                    `ServiceAffinity started with zeebe gateway ${gatewayAddress} and hazelcast client ${hazelcastOptions.clusterName}`,
                );
            })
            .catch((err) => {
                console.error(
                    `ServiceAffinity could not connect with zeebe gateway ${gatewayAddress} and hazelcast client ${hazelcastOptions.clusterName}. The following error occured ${err}`,
                );
            });
    }

    async createAffinityWorker(taskType: string): Promise<void> {
        // This worker is a publisher (in the pub/sub system)
        // (could be represented as a Send Task or a Service Task in the BPMN)
        // Refer to : https://docs.camunda.io/docs/components/modeler/bpmn/send-tasks/
        ServiceAffinity.zeebeClient.createWorker({
            taskType,
            taskHandler: async (job) => {
                try {
                    console.log(
                        `Publishing on topic ${job.processInstanceKey} started`,
                    );
                    const updatedVars = {
                        ...job?.variables,
                        processInstanceKey: job?.processInstanceKey,
                    };
                    const topic =
                        await ServiceAffinity.hazelcastClient.getReliableTopic(
                            job.processInstanceKey,
                        );
                    await topic.publish(updatedVars);
                    console.log(
                        `Publishing on topic ${job.processInstanceKey} is complete.`,
                    );
                    return await job.complete(updatedVars);
                } catch (error: any) {
                    console.error(
                        `Error while publishing message on topic ${job.processInstanceKey}`,
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
            const topic =
                await ServiceAffinity.hazelcastClient.getReliableTopic(
                    processInstanceKey,
                );

            this.affinityCallbacks[processInstanceKey] = cb;

            const listenerID = topic.addMessageListener(
                async (message: any) => {
                    console.log(
                        `[createProcessInstanceWithAffinity] Executing callback for topic ${processInstanceKey}`,
                    );
                    try {
                        await this.affinityCallbacks[processInstanceKey](
                            JSON.parse(message),
                        );
                    } catch (err) {
                        console.error(err);
                    }
                },
            );

            this.hazelcastListeners[processInstanceKey] = listenerID;
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

        // We want to maintain all the time one listener/subscriber
        // for each processInstanceKey / workflow
        // otherwise, if we publish a message, many listeners could be triggered
        // and execute differents callbacks, which is not what we want to do.
        // That's why we remove the old listener before adding a new one.

        // Remove the old subscriber / listener
        const currentListenerID = this.hazelcastListeners[processInstanceKey];
        topic.removeMessageListener(currentListenerID);

        // Register the new subscrier / listener
        const listenerID = topic.addMessageListener(async (message: any) => {
            console.log(
                `[publishMessageWithAffinity] Executing callback for topic ${processInstanceKey}`,
            );
            try {
                await this.affinityCallbacks[processInstanceKey](
                    JSON.parse(message),
                );
            } catch (err) {
                console.error(err);
            }
        });
        this.hazelcastListeners[processInstanceKey] = listenerID;
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
            delete this.affinityCallbacks[processInstanceKey];
            delete this.hazelcastListeners[processInstanceKey];
        } catch (err) {
            console.error(err);
        }
    }
}
