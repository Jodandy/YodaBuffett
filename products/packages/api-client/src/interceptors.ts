import type { AxiosInstance } from 'axios'

interface InterceptorCallbacks {
  onUnauthorized?: () => void
  onServerError?: (error: unknown) => void
}

export function setupRequestInterceptor(client: AxiosInstance): void {
  client.interceptors.request.use(
    (config) => {
      console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`)
      return config
    },
    (error) => {
      console.error('API Request Error:', error)
      return Promise.reject(error)
    }
  )
}

export function setupResponseInterceptor(
  client: AxiosInstance,
  callbacks: InterceptorCallbacks
): void {
  client.interceptors.response.use(
    (response) => {
      console.log(`API Response: ${response.status} ${response.config.url}`)
      return response
    },
    (error) => {
      console.error(
        'API Response Error:',
        error.response?.data || error.message
      )

      if (error.response?.status === 401) {
        callbacks.onUnauthorized?.()
      } else if (error.response?.status >= 500) {
        callbacks.onServerError?.(error)
      }

      return Promise.reject(error)
    }
  )
}
