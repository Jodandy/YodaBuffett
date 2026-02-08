# Products Monorepo

pnpm-based monorepo for YodaBuffett frontend applications and shared packages.

## Structure

```
products/
├── apps/                    # Deployable applications
│   ├── hub/                # Personal investment hub (port 3002)
│   ├── screener/           # Stock screener app (port 3000)
│   └── research/           # Research platform app (port 3001)
├── packages/               # Shared packages
│   ├── ui/                 # @yodabuffett/ui - Components, hooks, design system
│   ├── types/              # @yodabuffett/types - Shared TypeScript types
│   └── api-client/         # @yodabuffett/api-client - HTTP client utilities
├── package.json            # Root scripts
├── pnpm-workspace.yaml     # Workspace config
└── tsconfig.base.json      # Shared TS config
```

## Quick Commands

```bash
# From products/ root
pnpm install                 # Install all dependencies
pnpm dev:hub                # Run hub on localhost:3002
pnpm dev:screener           # Run screener on localhost:3000
pnpm dev:research           # Run research on localhost:3001
pnpm build:all              # Build everything
pnpm type-check             # TypeScript check all packages
pnpm lint                   # Lint all packages
pnpm clean                  # Remove all dist/node_modules
```

## Adding a New App

1. Create the app directory:
```bash
mkdir -p apps/my-app/src
```

2. Create `apps/my-app/package.json`:
```json
{
  "name": "yodabuffett-my-app",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc && vite build",
    "preview": "vite preview",
    "type-check": "tsc --noEmit",
    "lint": "eslint src --ext ts,tsx"
  },
  "dependencies": {
    "@yodabuffett/ui": "workspace:*",
    "@yodabuffett/api-client": "workspace:*",
    "@yodabuffett/types": "workspace:*",
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "react-router-dom": "^6.20.1"
  },
  "devDependencies": {
    "@types/react": "^18.2.37",
    "@types/react-dom": "^18.2.15",
    "@vitejs/plugin-react": "^4.1.1",
    "autoprefixer": "^10.4.16",
    "postcss": "^8.4.31",
    "tailwindcss": "^3.3.5",
    "typescript": "^5.2.2",
    "vite": "^4.5.0"
  }
}
```

3. Create `apps/my-app/vite.config.ts`:
```typescript
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3002,  // Pick unique port
    proxy: {
      '/api': {
        target: 'http://localhost:8002',  // Your backend
        changeOrigin: true,
      },
    },
  },
})
```

4. Create `apps/my-app/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "./dist",
    "rootDir": "./src"
  },
  "include": ["src"],
  "references": [
    { "path": "../../packages/ui" },
    { "path": "../../packages/types" },
    { "path": "../../packages/api-client" }
  ]
}
```

5. Create `apps/my-app/tailwind.config.js`:
```javascript
import uiPreset from '@yodabuffett/ui/tailwind.preset'

export default {
  presets: [uiPreset],
  content: [
    './src/**/*.{js,ts,jsx,tsx}',
    '../../packages/ui/src/**/*.{js,ts,jsx,tsx}',
  ],
}
```

6. Create entry files:
- `apps/my-app/src/main.tsx` - React entry point
- `apps/my-app/src/App.tsx` - Root component
- `apps/my-app/index.html` - HTML template

7. Add root script in `products/package.json`:
```json
"dev:my-app": "pnpm --filter yodabuffett-my-app dev"
```

8. Install and run:
```bash
pnpm install
pnpm dev:my-app
```

## Adding a Shared Package

1. Create package directory:
```bash
mkdir -p packages/my-package/src
```

2. Create `packages/my-package/package.json`:
```json
{
  "name": "@yodabuffett/my-package",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "./src/index.ts",
  "types": "./src/index.ts",
  "exports": {
    ".": "./src/index.ts"
  },
  "scripts": {
    "type-check": "tsc --noEmit"
  },
  "devDependencies": {
    "typescript": "^5.2.2"
  }
}
```

3. Create `packages/my-package/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "./dist",
    "rootDir": "./src"
  },
  "include": ["src"]
}
```

4. Create `packages/my-package/src/index.ts` with exports.

5. Use in apps:
```json
"dependencies": {
  "@yodabuffett/my-package": "workspace:*"
}
```

## Shared Packages Reference

### @yodabuffett/ui
Components, hooks, and design system.

**Components:**
- `MetricValue` - Display financial metrics
- `PerformanceIndicator` - Show performance status
- `LoadingSpinner` - Loading state
- `EmptyState` - Empty state UI
- `PageLayout` - Shared navigation layout

**Hooks:**
- `useSorting` - Table sorting logic
- `usePagination` - Pagination state

**Utils:**
- `cn()` - Class name utility (clsx wrapper)
- `formatCurrency()`, `formatPercentage()`, `formatNumber()`
- `getNestedValue()` - Safe object traversal

**Styles:**
- `@yodabuffett/ui/styles/base.css` - Tailwind base
- `@yodabuffett/ui/styles/components.css` - Component styles
- `@yodabuffett/ui/tailwind.preset` - Design system preset

**Usage:**
```tsx
import { PageLayout, MetricValue, cn, formatCurrency } from '@yodabuffett/ui'
import '@yodabuffett/ui/styles/base.css'
```

### @yodabuffett/types
Shared TypeScript types.

**Types:**
- `Company` - Company data structure
- `ApiResponse`, `ApiError` - API response wrappers
- `PaginationParams`, `PaginatedResponse` - Pagination
- `ColumnSort`, `TableFilter`, `PaginationOptions` - Table types
- `ExportRequest`, `ExportResponse` - Export types

**Usage:**
```tsx
import type { Company, ApiResponse, PaginatedResponse } from '@yodabuffett/types'
```

### @yodabuffett/api-client
HTTP client utilities.

**Exports:**
- `createApiClient(config)` - Create configured axios instance
- `isNetworkError()`, `isClientError()`, `isServerError()` - Error classification
- `getErrorMessage()`, `formatValidationErrors()` - Error formatting

**Usage:**
```tsx
import { createApiClient, getErrorMessage } from '@yodabuffett/api-client'

const api = createApiClient({ baseURL: '/api/v1' })
```

## Tech Stack

- **React 18** with TypeScript
- **Vite** for dev/build
- **Tailwind CSS** with shared preset
- **React Query** for data fetching (in apps)
- **Zustand** for state management (in apps)
- **React Router** for navigation

## State Management Architecture

Push state DOWN this hierarchy as far as possible:

```
┌─────────────────────────────────────────────────┐
│  URL State (React Router)                       │  ← Shareable, bookmarkable
│  - Current view, filters, selected items        │
├─────────────────────────────────────────────────┤
│  Server State (React Query)                     │  ← Cached, auto-refetch
│  - API data, mutations                          │
├─────────────────────────────────────────────────┤
│  Feature Stores (Zustand slices)                │  ← Cross-component state
│  - User preferences, UI mode, workspace state   │
├─────────────────────────────────────────────────┤
│  Component State (useState)                     │  ← Ephemeral, local only
│  - Dropdowns open, hover states                 │
└─────────────────────────────────────────────────┘
```

**Principle:** Most state should be URL or server state. Only use Zustand for truly global client concerns.

### Feature-Based Structure (Avoids Circular Dependencies)

```
src/
├── features/
│   ├── portfolio/          # Self-contained feature
│   │   ├── components/
│   │   ├── hooks/
│   │   ├── store.ts        # Feature-specific Zustand slice
│   │   └── api.ts
│   ├── watchlist/
│   └── alerts/
├── shared/                  # Truly shared, no feature imports
│   ├── components/
│   └── hooks/
└── app/                     # App shell, composes features
```

**Rule:** Features import from `shared/`, but NEVER from other features. Cross-feature communication goes through app layer or shared stores.

## Authentication Architecture

When auth is implemented, use `@yodabuffett/auth` package.

### Token Strategy

```
Login → API returns { accessToken, refreshToken, user }
                           ↓
┌─────────────────┐   ┌─────────────────┐   ┌───────────────┐
│ Access Token    │   │ Refresh Token   │   │ User + Tier   │
│ Memory only     │   │ HttpOnly cookie │   │ Zustand store │
│ (short-lived)   │   │ (secure)        │   │ (UI state)    │
└─────────────────┘   └─────────────────┘   └───────────────┘
```

- **Access token**: Memory only (not localStorage) - better XSS protection
- **Refresh token**: HttpOnly cookie - JS can't access
- **Silent refresh**: 401 triggers refresh, then retries original request

### Refresh Flow (in @yodabuffett/api-client)

```typescript
let accessToken: string | null = null
let refreshPromise: Promise<string> | null = null

apiClient.interceptors.response.use(
  (response) => response,
  async (error) => {
    if (error.response?.status === 401 && !error.config._retry) {
      error.config._retry = true

      // Dedupe concurrent refresh calls
      if (!refreshPromise) {
        refreshPromise = refreshAccessToken()
          .finally(() => { refreshPromise = null })
      }

      accessToken = await refreshPromise
      return apiClient(error.config)
    }
    return Promise.reject(error)
  }
)
```

### Tier-Based Feature Gating

```typescript
// User type
interface User {
  id: string
  email: string
  tier: 'free' | 'pro' | 'enterprise'
  features: string[]  // Explicit feature flags from backend
}

// Auth store exports
interface AuthStore {
  user: User | null
  isAuthenticated: boolean
  hasFeature: (feature: string) => boolean
  hasTier: (minTier: 'free' | 'pro' | 'enterprise') => boolean
}
```

### Feature Gate Component

```tsx
<FeatureGate feature="advanced-screener" fallback={<UpgradePrompt />}>
  <AdvancedScreener />
</FeatureGate>
```

### Two Layers of Gating (Always Both)

| Layer | Purpose | Example |
|-------|---------|---------|
| **UI** | Hide/disable features | Don't show "Export to Excel" button |
| **API** | Enforce server-side | Return 403 if free user hits premium endpoint |

Never trust UI alone - API must validate tier on every protected request.

### Future @yodabuffett/auth Package

```
packages/auth/
├── src/
│   ├── store.ts        # Auth Zustand store
│   ├── provider.tsx    # AuthProvider component
│   ├── hooks.ts        # useAuth, useUser, useFeature
│   ├── guards.tsx      # FeatureGate, RequireAuth
│   └── types.ts        # User, Tier, etc.
└── package.json
```

Usage across all apps:
```tsx
import { AuthProvider, useAuth, FeatureGate, RequireAuth } from '@yodabuffett/auth'
```

## Conventions

1. **Package naming:**
   - Apps: `yodabuffett-{name}` (e.g., `yodabuffett-screener`)
   - Packages: `@yodabuffett/{name}` (e.g., `@yodabuffett/ui`)

2. **Ports:**
   - screener: 3000
   - research: 3001
   - hub: 3002
   - New apps: 3003+

3. **Workspace dependencies:** Always use `workspace:*`

4. **Imports:** Use absolute imports from shared packages, relative within app

5. **Tailwind:** Always extend the UI preset for consistent design

## Existing Apps

| App | Port | Status | Description |
|-----|------|--------|-------------|
| hub | 3002 | In development | Personal investment command center |
| screener | 3000 | Production-ready | Stock screening with query builder, backtesting |
| research | 3001 | Scaffolded | Research platform (pages not implemented) |

## Hub App Architecture

The hub app uses a **sidebar layout** (different from screener's top-nav):

```
┌─────────────────────────────────────────────────────────────────────┐
│  TopBar: [        ] [ === Search stocks, portfolios... === ] [User]│
├────────────┬────────────────────────────────────────────────────────┤
│  Sidebar   │              Main Content Area                         │
│  ────────  │                                                        │
│  Dashboard │   (scrollable, max-width container)                    │
│  Portfolios│                                                        │
│  Watchlist │                                                        │
│  Alerts    │                                                        │
│  [collapse]│                                                        │
└────────────┴────────────────────────────────────────────────────────┘
```

**Structure:**
```
apps/hub/src/
├── app/shell/           # HubShell, Sidebar, TopBar, MainContent
├── features/
│   ├── dashboard/       # Home page
│   ├── screener/        # Fat Pitch screener grid
│   ├── company/         # Company detail page (/company/:symbol)
│   ├── portfolio/       # Multi-portfolio tracker
│   ├── watchlist/       # (planned)
│   └── alerts/          # (planned)
├── shared/              # App-specific shared components
└── services/            # API client
```

**Feature Module Pattern:**
Each feature is self-contained with its own pages, components, hooks, api, and types.
Features import from `shared/` but never from each other.

### Company Detail Feature

Route: `/company/:symbol` (e.g., `/company/ERIC-B`)

```
features/company/
├── api.ts                    # API functions for all company endpoints
├── hooks/useCompanyDetail.ts # React Query hooks
├── types/index.ts            # TypeScript types
├── components/
│   ├── CompanyHeader.tsx     # Name, symbol, stage badge, tier stars
│   ├── DimensionGrid.tsx     # All 14 dimension scores with bars
│   ├── PriceChart.tsx        # Interactive price chart (recharts)
│   ├── MetricCard.tsx        # Reusable metric display
│   └── CompanyTabs.tsx       # Tab navigation
└── pages/
    └── CompanyDetailPage.tsx # Main page with 4 tabs
```

**Tabs:**
- **Overview**: Price chart, key metrics, dimension scores, pitch summary
- **Financials**: Income statement, balance sheet, cash flow tables
- **Documents**: Annual reports, quarterly reports, press releases (Swedish companies)
- **Events**: Earnings dates, dividends, AGMs (Swedish companies)
