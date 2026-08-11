import type { ILoadOptionsFunctions } from 'n8n-workflow';
import type { Mock } from 'vitest';
import { mockDeep, type DeepMockProxy } from 'vitest-mock-extended';

import { searchPages, searchSpaces } from '../../methods/listSearch';
import { confluenceApiRequest } from '../../transport';

vi.mock('../../transport', async (importOriginal) => ({
	...(await importOriginal<object>()),
	confluenceApiRequest: vi.fn(),
}));

const apiRequest = confluenceApiRequest as unknown as Mock;
const nextLink = (cursor: string) => ({ next: `/wiki/api/v2/pages?limit=50&cursor=${cursor}` });

describe('Confluence listSearch', () => {
	let ctx: DeepMockProxy<ILoadOptionsFunctions>;

	beforeEach(() => {
		vi.clearAllMocks();
		ctx = mockDeep<ILoadOptionsFunctions>();
		ctx.getCurrentNodeParameter.mockReturnValue('111');
	});

	describe('searchSpaces', () => {
		beforeEach(() => {
			apiRequest.mockResolvedValue({
				results: [
					{ id: 111, key: 'ENG', name: 'Engineering' },
					{ id: 222, key: 'HR', name: 'People' },
				],
				_links: nextLink('next-cursor'),
			});
		});

		it('lists spaces with key-disambiguated labels and a pagination token', async () => {
			const result = await searchSpaces.call(ctx);

			expect(apiRequest).toHaveBeenCalledWith(
				'GET',
				'/wiki/api/v2/spaces',
				{},
				{ limit: 50, sort: 'name', status: 'current' },
			);
			expect(result.results).toEqual([
				{ name: 'Engineering (ENG)', value: '111' },
				{ name: 'People (HR)', value: '222' },
			]);
			expect(result.paginationToken).toBe('next-cursor');
		});

		it('filters by name on the fetched page', async () => {
			const result = await searchSpaces.call(ctx, 'eng');
			expect(result.results).toEqual([{ name: 'Engineering (ENG)', value: '111' }]);
		});

		it('returns no token on the last page', async () => {
			apiRequest.mockResolvedValue({ results: [], _links: {} });
			expect((await searchSpaces.call(ctx)).paginationToken).toBeUndefined();
		});
	});

	describe('searchPages', () => {
		beforeEach(() => {
			apiRequest.mockResolvedValue({
				results: [
					{ id: 98304, title: 'QA Root' },
					{ id: 98305, title: 'Meeting Notes' },
				],
				_links: nextLink('page-cursor'),
			});
		});

		it('scopes the listing to the selected space and returns the next cursor', async () => {
			const result = await searchPages.call(ctx);

			expect(ctx.getCurrentNodeParameter).toHaveBeenCalledWith('spaceId', { extractValue: true });
			expect(apiRequest).toHaveBeenCalledWith(
				'GET',
				'/wiki/api/v2/pages',
				{},
				{ limit: 50, sort: 'title', status: 'current', 'space-id': '111' },
			);
			expect(result.paginationToken).toBe('page-cursor');
		});

		it('lists site-wide when no space parameter is in scope', async () => {
			ctx.getCurrentNodeParameter.mockImplementation(() => {
				throw new Error('unknown parameter');
			});
			await searchPages.call(ctx);

			expect(apiRequest.mock.calls[0][3]).toEqual({ limit: 50, sort: 'title', status: 'current' });
		});

		it('filters by title on the fetched page', async () => {
			const result = await searchPages.call(ctx, 'meeting');
			expect(result.results).toEqual([{ name: 'Meeting Notes', value: '98305' }]);
		});
	});

	it.each([[searchSpaces], [searchPages]])(
		'%o forwards the pagination token as the cursor',
		async (search) => {
			apiRequest.mockResolvedValue({ results: [] });
			await search.call(ctx, undefined, 'prev-cursor');
			expect(apiRequest.mock.calls[0][3]).toMatchObject({ cursor: 'prev-cursor' });
		},
	);
});
