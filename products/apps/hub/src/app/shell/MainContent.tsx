import type { ReactNode } from 'react'

interface MainContentProps {
  children: ReactNode
}

export function MainContent({ children }: MainContentProps) {
  return (
    <main className="flex-1 overflow-auto bg-background">
      <div className="max-w-7xl mx-auto px-6 py-8">{children}</div>
    </main>
  )
}
