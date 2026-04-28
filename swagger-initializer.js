window.onload = function () {
    const config = {
      dom_id: '#swagger-ui',
      deepLinking: true,
      url: 'api.json'
    }
    const ui = SwaggerUIBundle(config)
  }