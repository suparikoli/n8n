import type { Logger } from 'n8n-workflow';
import { UserError } from 'n8n-workflow';
import { mock } from 'vitest-mock-extended';

import type { KafkaCredentials } from '../../../utils';
import { assertTopicExists } from '../../../v2/transport/admin';
import {
	confluentKafkaModuleMock,
	failNextAdminDisconnect,
	failNextTopicMetadata,
	getFakeAdmins,
	getFakeClientConfigs,
	resetConfluentKafkaRecordings,
	unknownTopicError,
} from '../../mocks/confluent-kafka';

vi.mock('@confluentinc/kafka-javascript', () => confluentKafkaModuleMock());

const credentials: KafkaCredentials = {
	clientId: 'n8n-test',
	brokers: 'localhost:9092',
	ssl: false,
	authentication: false,
};

const logger = mock<Logger>();

beforeEach(() => {
	resetConfluentKafkaRecordings();
	vi.clearAllMocks();
});

const lastAdmin = () => {
	const admin = getFakeAdmins().at(-1);
	if (!admin) throw new Error('the fake recorded no admin client');
	return admin;
};

describe('assertTopicExists', () => {
	it('asks the broker only about the topic the trigger will subscribe to', async () => {
		await assertTopicExists(credentials, 'my-topic', logger);

		expect(lastAdmin().fetchTopicMetadata).toHaveBeenCalledWith({
			topics: ['my-topic'],
			timeout: 10_000,
		});
	});

	it('builds the admin client from the same converted credential as the consumer', async () => {
		await assertTopicExists(credentials, 'my-topic', logger);

		expect(getFakeClientConfigs()).toStrictEqual([
			{
				kafkaJS: {
					brokers: ['localhost:9092'],
					clientId: 'n8n-test',
					ssl: false,
					logLevel: 1,
				},
			},
		]);
	});

	it('resolves quietly when the topic exists', async () => {
		await expect(assertTopicExists(credentials, 'my-topic', logger)).resolves.toBeUndefined();

		expect(logger.warn).not.toHaveBeenCalled();
	});

	it('fails when the broker does not know the topic, naming it and how to recover', async () => {
		failNextTopicMetadata(unknownTopicError());

		const assertion = assertTopicExists(credentials, 'missing-topic', logger);

		await expect(assertion).rejects.toThrow(UserError);
		await expect(assertion).rejects.toThrow('Kafka topic "missing-topic" does not exist');
		await expect(assertion).rejects.toMatchObject({
			description: expect.stringContaining('publish the workflow again'),
		});
	});

	it('proceeds on an inconclusive check rather than blocking activation', async () => {
		// A broker that cannot be reached is not proof of a missing topic, and the
		// consumer's own connect reports it with a better message a moment later.
		failNextTopicMetadata(new Error('Local: Broker transport failure'));

		await expect(assertTopicExists(credentials, 'my-topic', logger)).resolves.toBeUndefined();

		expect(logger.warn).toHaveBeenCalledWith(
			'Kafka topic could not be verified before starting the consumer',
			expect.objectContaining({ topic: 'my-topic' }),
		);
	});

	it('does not treat some other error code as a missing topic', async () => {
		failNextTopicMetadata(
			Object.assign(new Error('Broker: Group authorization failed'), { code: 30 }),
		);

		await expect(assertTopicExists(credentials, 'my-topic', logger)).resolves.toBeUndefined();
	});

	it('disconnects the admin client on every path', async () => {
		await assertTopicExists(credentials, 'my-topic', logger);
		expect(lastAdmin().disconnect).toHaveBeenCalledTimes(1);

		failNextTopicMetadata(unknownTopicError());
		await expect(assertTopicExists(credentials, 'missing-topic', logger)).rejects.toThrow(
			UserError,
		);
		expect(lastAdmin().disconnect).toHaveBeenCalledTimes(1);
	});

	it('reports the missing topic even if the admin disconnect then fails', async () => {
		failNextTopicMetadata(unknownTopicError());
		failNextAdminDisconnect(new Error('disconnect timed out'));

		await expect(assertTopicExists(credentials, 'missing-topic', logger)).rejects.toThrow(
			'Kafka topic "missing-topic" does not exist',
		);
	});

	it('works without a logger', async () => {
		failNextTopicMetadata(new Error('Local: Broker transport failure'));

		await expect(assertTopicExists(credentials, 'my-topic')).resolves.toBeUndefined();
	});
});
