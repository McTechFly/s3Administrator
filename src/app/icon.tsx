import { ImageResponse } from "next/og"

export const size = { width: 32, height: 32 }
export const contentType = "image/png"

export default function Icon() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          borderRadius: "6px",
          background:
            "linear-gradient(140deg, #0f172a 0%, #0c4a6e 48%, #0e7490 100%)",
          color: "white",
          fontSize: "18px",
          fontWeight: 700,
          fontFamily: "sans-serif",
          letterSpacing: "-0.5px",
        }}
      >
        S3
      </div>
    ),
    { ...size },
  )
}
