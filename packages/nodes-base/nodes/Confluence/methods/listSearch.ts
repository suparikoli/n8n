import type {
	IDataObject,
	ILoadOptionsFunctions,
	INodeListSearchItems,
	INodeListSearchResult,
} from 'n8n-workflow';

import { confluenceApiRequest } from '../transport';

const PAGE_SIZE = 50;

interface CursorPaginated {
	results?: IDataObject[];
	_links?: { next?: string };
}

function nextCursor(response: CursorPaginated): string | undefined {
	const next = response._links?.next;
	if (!next) return undefined;
	try {
		return new URL(next, 'https://api.atlassian.com').searchParams.get('cursor') ?? undefined;
	} catch {
		return undefined;
	}
}

export async function searchSpaces(
	this: ILoadOptionsFunctions,
	filter?: string,
	paginationToken?: string,
): Promise<INodeListSearchResult> {
	const qs: IDataObject = { limit: PAGE_SIZE, sort: 'name', status: 'current' };
	if (paginationToken) qs.cursor = paginationToken;

	const response = (await confluenceApiRequest.call(
		this,
		'GET',
		'/wiki/api/v2/spaces',
		{},
		qs,
	)) as CursorPaginated;

	const filterLower = filter?.trim().toLowerCase();
	const results: INodeListSearchItems[] = (response.results ?? [])
		.filter((space) => typeof space.id !== 'undefined' && typeof space.name === 'string')
		.filter((space) => !filterLower || (space.name as string).toLowerCase().includes(filterLower))
		.map((space) => ({
			name: `${space.name as string} (${(space.key as string) ?? space.id})`,
			value: String(space.id),
		}));

	return { results, paginationToken: nextCursor(response) };
}

export async function searchPages(
	this: ILoadOptionsFunctions,
	filter?: string,
	paginationToken?: string,
): Promise<INodeListSearchResult> {
	let spaceId = '';
	try {
		spaceId = (this.getCurrentNodeParameter('spaceId', { extractValue: true }) as string) ?? '';
	} catch {
		// no space parameter in scope
	}

	const qs: IDataObject = { limit: PAGE_SIZE, sort: 'title', status: 'current' };
	if (spaceId) qs['space-id'] = spaceId;
	if (paginationToken) qs.cursor = paginationToken;

	const response = (await confluenceApiRequest.call(
		this,
		'GET',
		'/wiki/api/v2/pages',
		{},
		qs,
	)) as CursorPaginated;

	const filterLower = filter?.trim().toLowerCase();
	const results: INodeListSearchItems[] = (response.results ?? [])
		.filter((page) => typeof page.id !== 'undefined' && typeof page.title === 'string')
		.filter((page) => !filterLower || (page.title as string).toLowerCase().includes(filterLower))
		.map((page) => ({
			name: page.title as string,
			value: String(page.id),
		}));

	return { results, paginationToken: nextCursor(response) };
}
