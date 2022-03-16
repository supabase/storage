module.exports = {
  apps: [
    {
      env: {
        NODE_ENV: 'production',
      },
      script: 'dist/server.js',
    },
  ],
}
