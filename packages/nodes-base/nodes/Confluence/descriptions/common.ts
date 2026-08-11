import type { INodeProperties } from 'n8n-workflow';

// Base selectors for the whole node family; operations spread these with their
// own name/description/displayOptions. searchPages scopes by a sibling parameter
// named exactly 'spaceId', so consumers must keep spaceRLC's name.
export const spaceRLC: INodeProperties = {
	displayName: 'Space',
	name: 'spaceId',
	type: 'resourceLocator',
	default: { mode: 'list', value: '' },
	required: true,
	description: 'The Confluence space',
	modes: [
		{
			displayName: 'From List',
			name: 'list',
			type: 'list',
			typeOptions: {
				searchListMethod: 'searchSpaces',
				searchable: true,
			},
		},
		{
			displayName: 'By ID',
			name: 'id',
			type: 'string',
			placeholder: 'e.g. 98432',
			validation: [
				{
					type: 'regex',
					properties: {
						regex: '^[0-9]+$',
						errorMessage: 'Not a valid Confluence Space ID (numeric)',
					},
				},
			],
		},
	],
};

export const pageRLC: INodeProperties = {
	displayName: 'Page',
	name: 'pageId',
	type: 'resourceLocator',
	default: { mode: 'list', value: '' },
	required: true,
	description: 'The Confluence page',
	modes: [
		{
			displayName: 'From List',
			name: 'list',
			type: 'list',
			typeOptions: {
				searchListMethod: 'searchPages',
				searchable: true,
			},
		},
		{
			displayName: 'By URL',
			name: 'url',
			type: 'string',
			placeholder: 'e.g. https://your-site.atlassian.net/wiki/spaces/KEY/pages/98304/Title',
			extractValue: {
				type: 'regex',
				regex: '/pages/(?:edit-v2/)?([0-9]+)',
			},
			validation: [
				{
					type: 'regex',
					properties: {
						regex: '.*/pages/(?:edit-v2/)?[0-9]+.*',
						errorMessage: 'Not a valid Confluence page URL (must contain /pages/<ID>)',
					},
				},
			],
		},
		{
			displayName: 'By ID',
			name: 'id',
			type: 'string',
			placeholder: 'e.g. 98304',
			validation: [
				{
					type: 'regex',
					properties: {
						regex: '^[0-9]+$',
						errorMessage: 'Not a valid Confluence Page ID (numeric)',
					},
				},
			],
		},
	],
};
