# Screener Frontend

Modern React + TypeScript application for the YodaBuffett Screener Pro.

## Overview

A professional web application featuring:
- Visual query builder for complex screening
- Interactive results table with sorting and filtering
- Historical backtesting interface
- Real-time data updates

## Tech Stack

- **React 18** with TypeScript
- **Vite** for fast development and building
- **Tailwind CSS** for styling
- **React Query** for data fetching
- **Zustand** for state management
- **Chart.js** for data visualization

## Setup

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build

# Run tests
npm test

# Type checking
npm run type-check
```

## Key Features

### Query Builder
- Visual interface for building complex screens
- Support for AND/OR logic combinations
- Relative metric comparisons
- Real-time validation

### Results Table
- Sortable columns with custom formatting
- Summary statistics (mean, median, win rate)
- Export functionality
- Forward return columns for backtesting

### Backtesting Interface
- Historical point-in-time screening
- Performance visualization
- Strategy comparison tools
- Downloadable reports

## Project Structure

```
src/
├── components/          # Reusable UI components
├── pages/              # Page components
├── hooks/              # Custom React hooks
├── services/           # API integration
├── types/              # TypeScript definitions
├── utils/              # Utility functions
└── styles/             # CSS and styling
```

## Development Guidelines

- Use TypeScript for all components
- Follow component composition patterns
- Implement proper error boundaries
- Write unit tests for business logic
- Use semantic HTML and ARIA labels