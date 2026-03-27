import { API_BASE_URL } from './env.js';

const TOKEN_KEY = 'provider_router_id_token';
const USER_KEY = 'provider_router_user';
const EXPIRES_AT_KEY = 'provider_router_expires_at';

export function getAuthToken() {
  return sessionStorage.getItem(TOKEN_KEY) || '';
}

export function getStoredUser() {
  const raw = sessionStorage.getItem(USER_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (_) {
    return null;
  }
}

export function storeAuthSession({ id_token, user, expires_at }) {
  if (id_token) sessionStorage.setItem(TOKEN_KEY, id_token);
  if (user) sessionStorage.setItem(USER_KEY, JSON.stringify(user));
  if (expires_at) sessionStorage.setItem(EXPIRES_AT_KEY, expires_at);
}

export function clearAuthSession() {
  sessionStorage.removeItem(TOKEN_KEY);
  sessionStorage.removeItem(USER_KEY);
  sessionStorage.removeItem(EXPIRES_AT_KEY);
}

export async function apiRequest(path, { method = 'GET', body, auth = true } = {}) {
  const headers = {
    'Content-Type': 'application/json',
  };

  if (auth) {
    const token = getAuthToken();
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }
  }

  const response = await fetch(`${API_BASE_URL}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  let payload = null;
  try {
    payload = await response.json();
  } catch (_) {
    payload = null;
  }

  if (!response.ok || !payload || payload.ok === false) {
    const code = payload?.error?.code || 'request_failed';
    const message = payload?.error?.message || `Request failed with status ${response.status}`;
    const error = new Error(message);
    error.code = code;
    error.status = response.status;

    if (auth && (response.status === 401 || code === 'invalid_token' || code === 'missing_token')) {
      clearAuthSession();
      if (!location.pathname.endsWith('/login.html') && !location.pathname.endsWith('login.html')) {
        sessionStorage.setItem('postLoginRedirect', location.pathname + location.search);
        location.href = './login.html';
      }
    }

    throw error;
  }

  return payload.data;
}
