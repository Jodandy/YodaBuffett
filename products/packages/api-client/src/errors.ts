/* eslint-disable @typescript-eslint/no-explicit-any */

export function isNetworkError(error: any): boolean {
  return !error.response && error.request
}

export function isClientError(error: any): boolean {
  return error.response?.status >= 400 && error.response?.status < 500
}

export function isServerError(error: any): boolean {
  return error.response?.status >= 500
}

export function getErrorMessage(error: any): string {
  if (error.response?.data?.message) {
    return error.response.data.message
  }
  if (error.response?.data?.errors?.length > 0) {
    return error.response.data.errors[0]
  }
  if (error.message) {
    return error.message
  }
  return 'An unexpected error occurred'
}

export function formatValidationErrors(error: any): string[] {
  if (error.response?.data?.errors) {
    return error.response.data.errors
  }
  return [getErrorMessage(error)]
}
