module.exports = {
  apps: [
    {
      env: {
        NODE_ENV: 'production',
      },
      exp_backoff_restart_delay: 100,
      script: 'dist/server.js',
    },
  ],
}
