import { ImageResponse } from "next/og"
import { SITE_NAME } from "@/lib/seo"

export const alt = `${SITE_NAME} social card`
export const size = {
  width: 1200,
  height: 630,
}
export const contentType = "image/png"

export default function OpenGraphImage() {
  return new ImageResponse(
    (
      <div
        style={{
          height: "100%",
          width: "100%",
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
          padding: "56px 64px",
          background:
            "linear-gradient(145deg, rgb(11, 18, 32) 0%, rgb(15, 118, 110) 45%, rgb(21, 94, 117) 100%)",
          color: "white",
          fontFamily: "Inter, ui-sans-serif, sans-serif",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: "20px",
            }}
          >
            <div
              style={{
                position: "relative",
                height: "68px",
                width: "68px",
                borderRadius: "20px",
                background:
                  "linear-gradient(160deg, rgb(8, 47, 73) 0%, rgb(15, 118, 110) 100%)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                overflow: "hidden",
              }}
            >
              <div
                style={{
                  position: "absolute",
                  width: "46px",
                  height: "16px",
                  borderRadius: "50%",
                  background: "rgb(153, 246, 228)",
                  top: "16px",
                }}
              />
              <div
                style={{
                  position: "absolute",
                  width: "46px",
                  height: "34px",
                  borderBottomLeftRadius: "22px",
                  borderBottomRightRadius: "22px",
                  background: "rgb(20, 184, 166)",
                  top: "22px",
                }}
              />
              <div
                style={{
                  position: "absolute",
                  width: "46px",
                  height: "16px",
                  borderRadius: "50%",
                  background: "rgb(45, 212, 191)",
                  top: "34px",
                }}
              />
            </div>
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                gap: "2px",
              }}
            >
              <div
                style={{
                  fontSize: "34px",
                  fontWeight: 700,
                  letterSpacing: "0.03em",
                }}
              >
                {SITE_NAME}
              </div>
              <div
                style={{
                  fontSize: "21px",
                  color: "rgba(224, 242, 254, 0.92)",
                }}
              >
                Open-source S3 file manager for real operations
              </div>
            </div>
          </div>
          <div
            style={{
              fontSize: "22px",
              backgroundColor: "rgba(11, 18, 32, 0.32)",
              border: "1px solid rgba(224, 242, 254, 0.25)",
              borderRadius: "999px",
              padding: "10px 18px",
              color: "rgb(224, 242, 254)",
            }}
          >
            AWS • Hetzner • R2 • MinIO
          </div>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              fontSize: "64px",
              fontWeight: 700,
              lineHeight: 1.05,
            }}
          >
            Bulk operations, search,
            <br />
            and safe cleanup
          </div>
          <div
            style={{
              fontSize: "30px",
              color: "rgba(224, 242, 254, 0.92)",
            }}
          >
            Manage S3 without scripts. Self-host free or use cloud.
          </div>
        </div>

        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "10px",
            flexWrap: "wrap",
            fontSize: "22px",
          }}
        >
          <div style={{ padding: "8px 14px", borderRadius: "999px", background: "rgba(11, 18, 32, 0.3)" }}>
            Bulk delete
          </div>
          <div style={{ padding: "8px 14px", borderRadius: "999px", background: "rgba(11, 18, 32, 0.3)" }}>
            Recursive cleanup
          </div>
          <div style={{ padding: "8px 14px", borderRadius: "999px", background: "rgba(11, 18, 32, 0.3)" }}>
            Sync & migrate
          </div>
          <div style={{ padding: "8px 14px", borderRadius: "999px", background: "rgba(11, 18, 32, 0.3)" }}>
            Encrypted credentials
          </div>
          <div style={{ padding: "8px 14px", borderRadius: "999px", background: "rgba(11, 18, 32, 0.3)" }}>
            Open source
          </div>
        </div>
      </div>
    ),
    {
      ...size,
    },
  )
}
