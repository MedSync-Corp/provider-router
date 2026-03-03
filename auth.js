import { apiRequest, clearAuthSession, getStoredUser, storeAuthSession } from './apiClient.js';

export async function getCurrentUser() {
  const cached = getStoredUser();
  if (cached) return cached;

  try {
    const me = await apiRequest('/auth/me');
    const user = { sub: me.sub, email: me.email };
    storeAuthSession({ user });
    return user;
  } catch (_) {
    return null;
  }
}

export async function requireAuth(redirect = './login.html') {
  const user = await getCurrentUser();
  if (!user) {
    sessionStorage.setItem('postLoginRedirect', location.pathname + location.search);
    location.href = redirect;
    throw new Error('Redirecting to login');
  }
  return user;
}

export async function signIn(email, password) {
  const data = await apiRequest('/auth/login', {
    method: 'POST',
    auth: false,
    body: { email, password },
  });

  const user = data.user || { email };
  storeAuthSession({
    id_token: data.id_token,
    user,
    expires_at: data.expires_at,
  });

  return user;
}

export async function signOut() {
  clearAuthSession();
  location.href = './login.html';
}

export function wireLogoutButton() {
  document.getElementById('logoutBtn')?.addEventListener('click', signOut);
}
