import type { Logger } from 'n8n-workflow';
import { UserError } from 'n8n-workflow';

import { createKafkaClient, getKafkaLibrary } from './client';
import type { KafkaCredentials } from '../../utils';

/** Bounds the metadata request so a slow broker cannot stall activation. */
const METADATA_TIMEOUT_MS = 10_000;

/**
 * Fails activation when the topic does not exist on the broker.
 *
 * Neither `subscribe()` nor `run()` rejects for a missing topic: librdkafka
 * resolves the subscription from metadata it does not have yet, so both resolve,
 * n8n reports the workflow as Published, and the fetch loop then logs
 * "Broker: Unknown topic or partition" on a retry the library never surfaces
 * (`_consumer.js:1532` swallows it because `restartOnFailure` is always true).
 * Until the subscription resolves, the consumer does not even join its group.
 *
 * It recovers on its own, but only at the next metadata refresh, which
 * `topic.metadata.refresh.interval.ms` puts 5 minutes out by default. Measured
 * against a real broker: a topic created 15s after activation was first consumed
 * 4m45s later, exactly 5 minutes after subscribe. So a Published workflow
 * consumes nothing for minutes, with no signal outside a debug log, and the wait
 * restarts if the topic appears after that window. v1 could not reach that
 * state: kafkajs resolved the subscription eagerly and refused to activate.
 *
 * Only an explicit "unknown topic" verdict blocks activation. Anything else
 * (broker unreachable, metadata denied, request timed out) is inconclusive
 * rather than proof of a missing topic, and the consumer's own connect reports
 * those with a better message a moment later, so the check stays out of the way.
 *
 * An admin metadata request does not create the topic, even on a broker with
 * `auto.create.topics.enable=true`: auto-creation is driven by the consumer's
 * own `allow.auto.create.topics`, which the library leaves off.
 * @param credentials - The decrypted Kafka credential
 * @param topic - The topic the trigger is about to subscribe to
 * @param logger - Records an inconclusive check, which is not an error
 */
export async function assertTopicExists(
	credentials: KafkaCredentials,
	topic: string,
	logger?: Logger,
): Promise<void> {
	const { ErrorCodes } = await getKafkaLibrary();
	const kafka = await createKafkaClient(credentials);
	const admin = kafka.admin();

	try {
		await admin.connect();
		await admin.fetchTopicMetadata({ topics: [topic], timeout: METADATA_TIMEOUT_MS });
	} catch (error) {
		if (isUnknownTopic(error, ErrorCodes.ERR_UNKNOWN_TOPIC_OR_PART)) {
			throw new UserError(`Kafka topic "${topic}" does not exist`, {
				level: 'warning',
				description:
					'A topic that is not on the broker yet would leave this workflow published but consuming nothing for several minutes. Create the topic (or correct the Topic field), then publish the workflow again.',
				cause: error instanceof Error ? error : undefined,
			});
		}

		logger?.warn('Kafka topic could not be verified before starting the consumer', {
			topic,
			error,
		});
	} finally {
		// Best effort: the verdict above is the useful outcome, and a failing
		// disconnect on a short-lived admin client must not mask it.
		await admin.disconnect().catch(() => {});
	}
}

/**
 * Whether the broker said the topic is unknown.
 *
 * Checked by code rather than message: the library's admin path preserves the
 * librdkafka code on the error it rejects with (verified against a real broker:
 * `KafkaJSProtocolError` with `code: 3`), unlike the consumer's log stream,
 * where the code is gone by the time it reaches a logger.
 */
function isUnknownTopic(error: unknown, unknownTopicCode: number): boolean {
	if (typeof error !== 'object' || error === null || !('code' in error)) return false;
	return error.code === unknownTopicCode;
}
