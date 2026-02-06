#!/bin/bash

# Build the app
npm run build

# Sync to S3
aws s3 sync dist/ s3://yodabuffett-screener --delete

# Invalidate CloudFront cache
aws cloudfront create-invalidation \
  --distribution-id YOUR_DISTRIBUTION_ID \
  --paths "/*"

echo "Deployment complete!"