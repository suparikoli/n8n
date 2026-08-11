import type { IDataObject, IExecuteFunctions, INodeProperties } from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

import { bodyProperties, readBodyEnvelope } from './bodyEnvelope';
import { pageRLC, spaceRLC } from '../../descriptions/common';
import { confluenceApiRequest } from '../../transport';

const showOnCreate = { resource: ['page'], operation: ['create'] };

export const description: INodeProperties[] = [
	{
		...spaceRLC,
		description: 'The space to create the page in',
		displayOptions: { show: showOnCreate },
	},
	{
		displayName: 'Title',
		name: 'title',
		type: 'string',
		default: '',
		required: true,
		placeholder: 'e.g. Weekly Report',
		description: 'The title of the new page',
		displayOptions: { show: showOnCreate },
	},
	...bodyProperties(['create']),
	{
		...pageRLC,
		displayName: 'Parent Page',
		name: 'parentPage',
		required: false,
		description:
			'The page to create the new page under. Leave empty to create under the space homepage.',
		displayOptions: {
			show: showOnCreate,
			// The API rejects root-level + parentId; hiding makes the combination unrepresentable
			hide: { '/options.rootLevel': [true] },
		},
	},
	{
		displayName: 'Options',
		name: 'options',
		type: 'collection',
		placeholder: 'Add Option',
		default: {},
		displayOptions: { show: showOnCreate },
		options: [
			{
				displayName: 'Create as Draft',
				name: 'createAsDraft',
				type: 'boolean',
				default: false,
				description: 'Whether to create the page as a draft instead of publishing it',
			},
			{
				displayName: 'Private',
				name: 'private',
				type: 'boolean',
				default: false,
				description:
					'Whether only the creating user can view and edit the page. The creator is the connected account.',
			},
			{
				displayName: 'Root Level',
				name: 'rootLevel',
				type: 'boolean',
				default: false,
				description:
					'Whether to create the page at the space root, outside the space homepage tree. Cannot be combined with a parent page.',
			},
		],
	},
];

export async function execute(
	this: IExecuteFunctions,
	itemIndex: number,
): Promise<IDataObject | IDataObject[]> {
	const spaceId = this.getNodeParameter('spaceId', itemIndex, undefined, {
		extractValue: true,
	}) as string;
	const options = this.getNodeParameter('options', itemIndex, {});

	const rawTitle: unknown = this.getNodeParameter('title', itemIndex, '');
	const title =
		typeof rawTitle === 'string'
			? rawTitle.trim()
			: rawTitle === null || rawTitle === undefined || typeof rawTitle === 'object'
				? ''
				: String(rawTitle).trim();

	if (!spaceId) {
		throw new NodeOperationError(this.getNode(), 'Space is required', { itemIndex });
	}
	if (!title) {
		throw new NodeOperationError(this.getNode(), 'Title is required', { itemIndex });
	}

	const body: IDataObject = {
		spaceId,
		status: options.createAsDraft ? 'draft' : 'current',
		title,
		body: readBodyEnvelope(this, itemIndex),
	};

	if (!options.rootLevel) {
		const parentId = this.getNodeParameter('parentPage', itemIndex, '', {
			extractValue: true,
		}) as string;
		if (parentId) body.parentId = parentId;
	}

	const qs: IDataObject = {};
	if (options.private) qs.private = true;
	if (options.rootLevel) qs['root-level'] = true;

	return await confluenceApiRequest.call(this, 'POST', '/wiki/api/v2/pages', body, qs);
}
