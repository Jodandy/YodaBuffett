export interface ExportRequest {
  format: 'csv' | 'xlsx' | 'json'
  includeMetadata: boolean
}

export interface ExportResponse {
  downloadUrl: string
  filename: string
  expiresAt: string
}
