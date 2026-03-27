import { apiRequest } from './apiClient.js';

export async function getDataClient() {
  return {
    from(table) {
      return new QueryBuilder(table);
    },
  };
}

class QueryBuilder {
  constructor(table) {
    this.table = table;
    this.action = null;
    this.columns = '*';
    this.returning = '*';
    this.selectOptions = {};
    this.payload = null;
    this.filters = [];
    this.orders = [];
    this.limitValue = null;
    this.singleMode = null;
  }

  select(columns = '*', options = {}) {
    if (!this.action || this.action === 'select') {
      this.action = 'select';
      this.selectOptions = options || {};
    }
    this.columns = columns;
    this.returning = columns;
    return this;
  }

  insert(payload) {
    this.action = 'insert';
    this.payload = payload;
    return this;
  }

  update(payload) {
    this.action = 'update';
    this.payload = payload;
    return this;
  }

  delete() {
    this.action = 'delete';
    return this;
  }

  eq(field, value) {
    this.filters.push({ op: 'eq', field, value });
    return this;
  }

  in(field, values) {
    this.filters.push({ op: 'in', field, values: Array.isArray(values) ? values : [] });
    return this;
  }

  lte(field, value) {
    this.filters.push({ op: 'lte', field, value });
    return this;
  }

  gt(field, value) {
    this.filters.push({ op: 'gt', field, value });
    return this;
  }

  order(field, options = {}) {
    this.orders.push({ field, ascending: options.ascending !== false });
    return this;
  }

  limit(value) {
    this.limitValue = Number(value) || null;
    return this;
  }

  single() {
    this.singleMode = 'single';
    return this;
  }

  maybeSingle() {
    this.singleMode = 'maybeSingle';
    return this;
  }

  then(resolve, reject) {
    return this.execute().then(resolve, reject);
  }

  catch(reject) {
    return this.execute().catch(reject);
  }

  finally(handler) {
    return this.execute().finally(handler);
  }

  async execute() {
    try {
      const result = await runQuery(this);
      return normalizeSingleResult(result, this.singleMode);
    } catch (error) {
      return { data: null, error };
    }
  }
}

async function runQuery(query) {
  switch (query.action) {
    case 'select':
      return runSelect(query);
    case 'insert':
      return runInsert(query);
    case 'update':
      return runUpdate(query);
    case 'delete':
      return runDelete(query);
    default:
      return { data: null, error: new Error(`Unsupported action for ${query.table}`) };
  }
}

async function runSelect(query) {
  switch (query.table) {
    case 'providers':
      return selectProviders(query);
    case 'provider_details':
      return selectProviderDetails(query);
    case 'provider_licenses':
      return selectProviderLicenses(query);
    case 'provider_credentials':
      return selectProviderCredentials(query);
    case 'credential_linking':
      return selectCredentialLinking(query);
    case 'group_enrollments':
      return selectGroupEnrollments(query);
    case 'onboarding_tasks':
      return selectOnboardingTasks(query);
    case 'contact_notes':
      return selectContactNotes(query);
    case 'status_log':
      return selectStatusLog(query);
    case 'provider_pipeline':
      return selectProviderPipeline(query);
    case 'practice_locations':
      return selectPracticeLocations(query);
    case 'payer_contacts':
      return selectPayerContacts(query);
    case 'routing_log':
      return selectRoutingLog(query);
    case 'action_items':
      return selectManualActionItems(query);
    default:
      return { data: [], error: null };
  }
}

async function runInsert(query) {
  switch (query.table) {
    case 'providers':
      return { data: await apiRequest('/providers', { method: 'POST', body: query.payload }), error: null };
    case 'provider_details':
      return { data: await apiRequest('/provider-details', { method: 'POST', body: query.payload }), error: null };
    case 'provider_licenses':
      return { data: await apiRequest('/provider-licenses', { method: 'POST', body: query.payload }), error: null };
    case 'provider_credentials': {
      const providerID = query.payload?.provider_id;
      return {
        data: await apiRequest(`/providers/${providerID}/credentials`, {
          method: 'POST',
          body: omitKeys(query.payload, ['provider_id']),
        }),
        error: null,
      };
    }
    case 'credential_linking': {
      const credentialID = query.payload?.credential_id;
      await apiRequest(`/credentials/${credentialID}/linking`, {
        method: 'PUT',
        body: { linked: !!query.payload?.linked },
      });
      const { data } = await selectCredentialLinking(new QueryBuilder('credential_linking').select('*').eq('credential_id', credentialID).maybeSingle());
      return { data, error: null };
    }
    case 'group_enrollments':
      return { data: await apiRequest('/group-enrollments', { method: 'POST', body: query.payload }), error: null };
    case 'onboarding_tasks': {
      if (Array.isArray(query.payload)) {
        const items = [];
        for (const row of query.payload) {
          items.push(await apiRequest('/onboarding-tasks', { method: 'POST', body: row }));
        }
        return { data: items, error: null };
      }
      return { data: await apiRequest('/onboarding-tasks', { method: 'POST', body: query.payload }), error: null };
    }
    case 'contact_notes': {
      const providerID = query.payload?.provider_id;
      return {
        data: await apiRequest(`/providers/${providerID}/notes`, {
          method: 'POST',
          body: { note: query.payload?.note || '' },
        }),
        error: null,
      };
    }
    case 'status_log':
    case 'routing_log':
      return { data: query.payload, error: null };
    case 'provider_pipeline':
      return { data: await apiRequest('/pipeline', { method: 'POST', body: query.payload }), error: null };
    case 'practice_locations':
      return { data: await apiRequest('/settings/practice-locations', { method: 'POST', body: query.payload }), error: null };
    case 'payer_contacts':
      return { data: await apiRequest('/settings/payer-contacts', { method: 'POST', body: query.payload }), error: null };
    case 'action_items':
      return { data: await apiRequest('/action-items', { method: 'POST', body: query.payload }), error: null };
    default:
      return { data: null, error: new Error(`Unsupported insert on ${query.table}`) };
  }
}

async function runUpdate(query) {
  const id = getEqValue(query, 'id');
  switch (query.table) {
    case 'providers': {
      if (!id) return { data: null, error: new Error('Missing id filter') };
      if (Object.keys(query.payload || {}).length === 1 && Object.prototype.hasOwnProperty.call(query.payload, 'status')) {
        return {
          data: await apiRequest(`/providers/${id}/status`, {
            method: 'PATCH',
            body: { status: query.payload.status },
          }),
          error: null,
        };
      }
      const existing = await selectSingleRow('providers', id);
      return {
        data: await apiRequest(`/providers/${id}`, {
          method: 'PUT',
          body: { ...existing, ...query.payload },
        }),
        error: null,
      };
    }
    case 'provider_details': {
      const existing = await selectSingleRow('provider_details', id);
      return { data: await apiRequest(`/provider-details/${id}`, { method: 'PUT', body: { ...existing, ...query.payload } }), error: null };
    }
    case 'provider_licenses': {
      const existing = await selectSingleRow('provider_licenses', id);
      return { data: await apiRequest(`/provider-licenses/${id}`, { method: 'PUT', body: { ...existing, ...query.payload } }), error: null };
    }
    case 'provider_credentials': {
      const existing = await selectSingleRow('provider_credentials', id);
      return {
        data: await apiRequest(`/providers/${existing.provider_id}/credentials/${id}`, {
          method: 'PUT',
          body: { ...omitKeys(existing, ['providers', 'credential_linking']), ...query.payload },
        }),
        error: null,
      };
    }
    case 'credential_linking': {
      const credentialID = getEqValue(query, 'credential_id') || (await resolveCredentialIDByLinkingID(id));
      return {
        data: await apiRequest(`/credentials/${credentialID}/linking`, {
          method: 'PUT',
          body: { linked: !!query.payload?.linked },
        }),
        error: null,
      };
    }
    case 'group_enrollments': {
      const existing = await selectSingleRow('group_enrollments', id);
      return { data: await apiRequest(`/group-enrollments/${id}`, { method: 'PUT', body: { ...existing, ...query.payload } }), error: null };
    }
    case 'onboarding_tasks': {
      const existing = await selectSingleRow('onboarding_tasks', id);
      return { data: await apiRequest(`/onboarding-tasks/${id}`, { method: 'PUT', body: { ...existing, ...query.payload } }), error: null };
    }
    case 'provider_pipeline': {
      const existing = await selectSingleRow('provider_pipeline', id);
      return { data: await apiRequest(`/pipeline/${id}`, { method: 'PUT', body: { ...existing, ...query.payload } }), error: null };
    }
    case 'practice_locations': {
      const existing = await selectSingleRow('practice_locations', id);
      return { data: await apiRequest(`/settings/practice-locations/${id}`, { method: 'PUT', body: { ...existing, ...query.payload } }), error: null };
    }
    case 'payer_contacts': {
      const existing = await selectSingleRow('payer_contacts', id);
      return { data: await apiRequest(`/settings/payer-contacts/${id}`, { method: 'PUT', body: { ...existing, ...query.payload } }), error: null };
    }
    case 'action_items': {
      const existing = await selectSingleRow('action_items', id);
      const base = existing?.dbRecord || existing || {};
      return { data: await apiRequest(`/action-items/${id}`, { method: 'PUT', body: { ...base, ...query.payload } }), error: null };
    }
    case 'status_log':
    case 'routing_log':
      return { data: query.payload, error: null };
    default:
      return { data: null, error: new Error(`Unsupported update on ${query.table}`) };
  }
}

async function runDelete(query) {
  const id = getEqValue(query, 'id');
  switch (query.table) {
    case 'providers':
      return { data: await apiRequest(`/providers/${id}`, { method: 'DELETE' }), error: null };
    case 'provider_licenses':
      return { data: await apiRequest(`/provider-licenses/${id}`, { method: 'DELETE' }), error: null };
    case 'provider_credentials': {
      const existing = await selectSingleRow('provider_credentials', id);
      return { data: await apiRequest(`/providers/${existing.provider_id}/credentials/${id}`, { method: 'DELETE' }), error: null };
    }
    case 'credential_linking':
    case 'status_log':
    case 'routing_log':
      return { data: { deleted: true }, error: null };
    case 'group_enrollments':
      return { data: await apiRequest(`/group-enrollments/${id}`, { method: 'DELETE' }), error: null };
    case 'provider_pipeline':
      return { data: await apiRequest(`/pipeline/${id}`, { method: 'DELETE' }), error: null };
    case 'practice_locations':
      return { data: await apiRequest(`/settings/practice-locations/${id}`, { method: 'DELETE' }), error: null };
    case 'payer_contacts':
      return { data: await apiRequest(`/settings/payer-contacts/${id}`, { method: 'DELETE' }), error: null };
    case 'action_items':
      return { data: await apiRequest(`/action-items/${id}`, { method: 'DELETE' }), error: null };
    default:
      return { data: null, error: new Error(`Unsupported delete on ${query.table}`) };
  }
}

async function selectProviders(query) {
  if (query.selectOptions?.head && query.selectOptions?.count === 'exact') {
    const summary = await apiRequest('/summary');
    return { data: null, error: null, count: summary.total || 0 };
  }
  const id = getEqValue(query, 'id');
  if (id) {
    const bundle = await apiRequest(`/providers/${id}`);
    return { data: [bundle.provider], error: null };
  }
  const result = await apiRequest('/providers?page=1&page_size=1000');
  const rows = sortRows(applyFilters(result.items || [], query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null, count: result.total || rows.length };
}

async function selectProviderDetails(query) {
  const params = new URLSearchParams();
  const providerID = getEqValue(query, 'provider_id');
  const id = getEqValue(query, 'id');
  if (providerID) params.set('provider_id', providerID);
  if (id) params.set('id', id);
  const result = await apiRequest(`/provider-details${params.toString() ? `?${params.toString()}` : ''}`);
  const rows = sortRows(applyFilters(result.items || [], query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectProviderLicenses(query) {
  const params = new URLSearchParams();
  const providerID = getEqValue(query, 'provider_id');
  const id = getEqValue(query, 'id');
  if (providerID) params.set('provider_id', providerID);
  if (id) params.set('id', id);
  const result = await apiRequest(`/provider-licenses${params.toString() ? `?${params.toString()}` : ''}`);
  let rows = result.items || [];
  if (query.columns.includes('providers')) {
    const providerMap = await fetchProviderMap();
    rows = rows.map((row) => ({ ...row, providers: providerMap.get(row.provider_id) || null }));
  }
  rows = sortRows(applyFilters(rows, query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectProviderCredentials(query) {
  const params = new URLSearchParams();
  for (const key of ['id', 'provider_id', 'state', 'payer', 'status']) {
    const value = getEqValue(query, key);
    if (value) params.set(key, value);
  }
  const result = await apiRequest(`/credentials${params.toString() ? `?${params.toString()}` : ''}`);
  const rows = sortRows(applyFilters(result.items || [], query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectCredentialLinking(query) {
  const params = new URLSearchParams();
  const credentialID = getEqValue(query, 'credential_id');
  const id = getEqValue(query, 'id');
  if (credentialID) params.set('credential_id', credentialID);
  if (id) params.set('id', id);
  const result = await apiRequest(`/credential-linking${params.toString() ? `?${params.toString()}` : ''}`);
  const rows = sortRows(applyFilters(result.items || [], query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectGroupEnrollments(query) {
  const params = new URLSearchParams();
  for (const key of ['id', 'state', 'payer']) {
    const value = getEqValue(query, key);
    if (value) params.set(key, value);
  }
  const result = await apiRequest(`/group-enrollments${params.toString() ? `?${params.toString()}` : ''}`);
  const rows = sortRows(applyFilters(result.items || [], query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectOnboardingTasks(query) {
  const params = new URLSearchParams();
  const providerID = getEqValue(query, 'provider_id');
  const id = getEqValue(query, 'id');
  if (providerID) params.set('provider_id', providerID);
  if (id) params.set('id', id);
  const result = await apiRequest(`/onboarding-tasks${params.toString() ? `?${params.toString()}` : ''}`);
  let rows = result.items || [];
  if (query.columns.includes('providers')) {
    const providerMap = await fetchProviderMap();
    rows = rows.map((row) => ({ ...row, providers: providerMap.get(row.provider_id) || null }));
  }
  rows = sortRows(applyFilters(rows, query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectContactNotes(query) {
  const providerID = getEqValue(query, 'provider_id');
  if (!providerID) return { data: [], error: null };
  const bundle = await apiRequest(`/providers/${providerID}`);
  const rows = sortRows(applyFilters(bundle.contact_notes || bundle.notes || [], query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectStatusLog(query) {
  const ids = getInValues(query, 'entity_id');
  const singleID = getEqValue(query, 'entity_id');
  const entityType = getEqValue(query, 'entity_type');
  const items = [];

  if (ids.length > 0) {
    for (const id of ids) {
      const params = new URLSearchParams({ entity_id: id });
      if (entityType) params.set('entity_type', entityType);
      const result = await apiRequest(`/status-log?${params.toString()}`);
      items.push(...(result.items || []));
    }
  } else {
    const params = new URLSearchParams();
    if (singleID) params.set('entity_id', singleID);
    if (entityType) params.set('entity_type', entityType);
    const result = await apiRequest(`/status-log${params.toString() ? `?${params.toString()}` : ''}`);
    items.push(...(result.items || []));
  }

  const deduped = dedupeByID(items);
  const rows = sortRows(applyFilters(deduped, query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectProviderPipeline(query) {
  const params = new URLSearchParams();
  const id = getEqValue(query, 'id');
  if (id) params.set('id', id);
  const result = await apiRequest(`/pipeline${params.toString() ? `?${params.toString()}` : ''}`);
  const rows = sortRows(applyFilters(result.items || [], query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectPracticeLocations(query) {
  const result = await apiRequest('/settings/practice-locations');
  const rows = sortRows(applyFilters(result.items || [], query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectPayerContacts(query) {
  const result = await apiRequest('/settings/payer-contacts');
  const rows = sortRows(applyFilters(result.items || [], query.filters), query.orders);
  return { data: sliceRows(rows, query.limitValue), error: null };
}

async function selectRoutingLog(query) {
  const limit = query.limitValue || 50;
  const result = await apiRequest(`/dashboard/activity?limit=${limit}`);
  const rows = (result.items || []).map((item) => ({
    ...item,
    providers: item.matched_provider_name ? splitProviderName(item.matched_provider_name) : null,
  }));
  return { data: sortRows(rows, query.orders), error: null };
}

async function selectManualActionItems(query) {
  const result = await apiRequest('/action-items');
  const rows = (result.items || [])
    .filter((item) => item.type === 'manual' || item.dbRecord)
    .map((item) => item.dbRecord || {
      id: item.id,
      title: item.title,
      description: item.description,
      priority: item.priority,
      due_date: item.dueDate || null,
      assigned_to: item.assignedTo || null,
      status: item.status || 'Open',
      created_by: item.createdBy || null,
      created_at: item.createdAt || null,
      type: 'manual',
    });
  return { data: sliceRows(sortRows(applyFilters(rows, query.filters), query.orders), query.limitValue), error: null };
}

function normalizeSingleResult(result, singleMode) {
  if (!singleMode) return result;
  const rows = Array.isArray(result.data) ? result.data : result.data == null ? [] : [result.data];
  if (singleMode === 'maybeSingle') {
    return { ...result, data: rows[0] || null };
  }
  if (rows.length === 0) {
    return { data: null, error: new Error('Expected a single row') };
  }
  return { ...result, data: rows[0] };
}

function applyFilters(rows, filters) {
  return rows.filter((row) => filters.every((filter) => matchesFilter(row, filter)));
}

function matchesFilter(row, filter) {
  const value = row?.[filter.field];
  switch (filter.op) {
    case 'eq':
      return value === filter.value;
    case 'in':
      return filter.values.includes(value);
    case 'lte':
      return value != null && String(value) <= String(filter.value);
    case 'gt':
      return value != null && String(value) > String(filter.value);
    default:
      return true;
  }
}

function sortRows(rows, orders) {
  if (!orders.length) return rows;
  return [...rows].sort((left, right) => {
    for (const order of orders) {
      const a = normalizeSortValue(left?.[order.field]);
      const b = normalizeSortValue(right?.[order.field]);
      if (a < b) return order.ascending ? -1 : 1;
      if (a > b) return order.ascending ? 1 : -1;
    }
    return 0;
  });
}

function normalizeSortValue(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value.toLowerCase();
  return value;
}

function sliceRows(rows, limit) {
  if (!limit || rows.length <= limit) return rows;
  return rows.slice(0, limit);
}

function getEqValue(query, field) {
  const match = query.filters.find((filter) => filter.op === 'eq' && filter.field === field);
  return match ? match.value : '';
}

function getInValues(query, field) {
  const match = query.filters.find((filter) => filter.op === 'in' && filter.field === field);
  return match ? match.values : [];
}

async function selectSingleRow(table, id) {
  const builder = new QueryBuilder(table).select('*').eq('id', id).single();
  const result = await runSelect(builder);
  const normalized = normalizeSingleResult(result, 'single');
  if (normalized.error) throw normalized.error;
  return normalized.data;
}

async function fetchProviderMap() {
  const result = await apiRequest('/providers?page=1&page_size=1000');
  const map = new Map();
  for (const provider of result.items || []) {
    map.set(provider.id, provider);
  }
  return map;
}

async function resolveCredentialIDByLinkingID(id) {
  const result = await selectCredentialLinking(new QueryBuilder('credential_linking').select('*').eq('id', id).single());
  return result.data?.credential_id || '';
}

function dedupeByID(items) {
  const seen = new Map();
  for (const item of items) {
    if (item?.id) seen.set(item.id, item);
  }
  return Array.from(seen.values());
}

function splitProviderName(name) {
  const trimmed = String(name || '').trim();
  if (!trimmed) return null;
  const parts = trimmed.split(/\s+/);
  if (parts.length === 1) {
    return { first_name: parts[0], last_name: '' };
  }
  return {
    first_name: parts.slice(0, -1).join(' '),
    last_name: parts[parts.length - 1],
  };
}

function omitKeys(value, keys) {
  const clone = { ...(value || {}) };
  for (const key of keys) {
    delete clone[key];
  }
  return clone;
}
