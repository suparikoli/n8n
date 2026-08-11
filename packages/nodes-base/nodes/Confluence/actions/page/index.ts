import type { INodeProperties } from 'n8n-workflow';

import * as create from './create.operation';

export { create };

export const pageProperties: INodeProperties[] = [
	{
		displayName: 'Operation',
		name: 'operation',
		type: 'options',
		noDataExpression: true,
		default: 'create',
		displayOptions: { show: { resource: ['page'] } },
		options: [
			{
				name: 'Create',
				value: 'create',
				description: 'Create a new page in a space',
				action: 'Create a page',
			},
		],
	},
	...create.description,
];
