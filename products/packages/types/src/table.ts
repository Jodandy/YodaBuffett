export interface ColumnSort {
  column: string
  direction: 'asc' | 'desc'
}

export interface TableFilter {
  column: string
  value: string | number
  operator: string
}

export interface PaginationOptions {
  page: number
  pageSize: number
  total: number
}
