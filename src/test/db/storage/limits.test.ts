import { isValidKey } from "@storage/limits"

describe("Testing limits", () => {
  test("accept special characters as s3 object name", () => {
    expect(isValidKey("望舌诊病.pdf")).toBe(true)
    expect(isValidKey("ÖÄÜ.jpg")).toBe(true)
    expect(isValidKey("åäö.png")).toBe(true)
  })
})
