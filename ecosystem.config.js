module.exports = {
  apps: [
    {
      instances: 'max',
      env: {
        NODE_ENV: 'production',
      },
      script: 'dist/server.js',
    },
  ],
}
