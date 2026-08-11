import type { IDataObject, IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

import * as page from './page';

/** The router calls `<resource>.<operation>.execute.call(this, i)` once per item. */
export type ConfluenceOperation = (
	this: IExecuteFunctions,
	itemIndex: number,
) => Promise<IDataObject | IDataObject[]>;

const operations: Record<string, Record<string, ConfluenceOperation>> = {
	page: {
		create: page.create.execute,
	},
};

export async function router(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
	const items = this.getInputData();
	const returnData: INodeExecutionData[] = [];

	for (let i = 0; i < items.length; i++) {
		try {
			const resource = this.getNodeParameter('resource', i, '');
			const operation = this.getNodeParameter('operation', i, '');

			const resourceOperations = Object.hasOwn(operations, resource)
				? operations[resource]
				: undefined;
			const execute =
				resourceOperations && Object.hasOwn(resourceOperations, operation)
					? resourceOperations[operation]
					: undefined;
			if (!execute) {
				throw new NodeOperationError(
					this.getNode(),
					`The operation "${resource}:${operation}" is not supported`,
					{ itemIndex: i },
				);
			}

			const result = await execute.call(this, i);
			const executionData = this.helpers.constructExecutionMetaData(
				this.helpers.returnJsonArray(result),
				{ itemData: { item: i } },
			);
			returnData.push.apply(returnData, executionData);
		} catch (error) {
			if (this.continueOnFail()) {
				returnData.push({
					json: { error: (error as Error).message },
					pairedItem: { item: i },
				});
				continue;
			}
			throw error;
		}
	}

	return [returnData];
}
