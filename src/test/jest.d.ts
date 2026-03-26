declare namespace jest {
  function isolateModulesAsync(fn: () => Promise<void>): Promise<void>
}
