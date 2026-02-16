import { ImageResponse } from "next/og"

export const size = { width: 180, height: 180 }
export const contentType = "image/png"

export default function AppleIcon() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          borderRadius: "40px",
          background:
            "linear-gradient(140deg, #0f172a 0%, #0c4a6e 48%, #0e7490 100%)",
          color: "white",
          fontSize: "96px",
          fontWeight: 700,
          fontFamily: "sans-serif",
          letterSpacing: "-2px",
        }}
      >
        S3
      </div>
    ),
    { ...size },
  )
}
