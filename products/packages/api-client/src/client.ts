import axios, { type AxiosInstance } from 'axios'
import { setupRequestInterceptor, setupResponseInterceptor } from './interceptors'

export interface ApiClientConfig {
  baseURL: string
  timeout?: number
  onUnauthorized?: () => void
  onServerError?: (error: unknown) => void
}

export function createApiClient(config: ApiClientConfig): AxiosInstance {
  const client = axios.create({
    baseURL: config.baseURL,
    timeout: config.timeout ?? 30000,
    headers: {
      'Content-Type': 'application/json',
    },
  })

  setupRequestInterceptor(client)
  setupResponseInterceptor(client, {
    onUnauthorized: config.onUnauthorized,
    onServerError: config.onServerError,
  })

  return client
}
