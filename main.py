"""
main.py — v2
Entry point for Cloud Run HTTP trigger and local CLI.

New flags:
  --passes N          Override passes per prompt (default from .env PASSES_PER_PROMPT)
  --competitor-id N   Run same prompts targeting a competitor brand
  --onboard           Run full onboarding research pipeline

Examples:
  python main.py --brand-id 1                         # standard run
  python main.py --brand-id 1 --passes 5             # 5 passes per prompt
  python main.py --brand-id 1 --competitor-id 2      # competitor gap run
  python main.py --onboard --brand-id 1 --seed "blueprint printing"
"""
import os
import sys
import json
import argparse
from http.server import HTTPServer, BaseHTTPRequestHandler

from runner.run import run_brand, run_all_brands


class RunHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Rankovi GEO Engine v2 - OK")

    def do_POST(self):
        if self.path != "/run":
            self.send_response(404); self.end_headers(); return

        length  = int(self.headers.get("Content-Length", 0))
        body    = self.rfile.read(length) if length else b"{}"
        payload = json.loads(body)

        try:
            brand_id = payload.get("brand_id")
            passes   = payload.get("passes")
            if brand_id:
                run_brand(int(brand_id), triggered_by="scheduler", passes=passes)
            else:
                run_all_brands(triggered_by="scheduler", passes=passes)
            self.send_response(200); self.end_headers()
            self.wfile.write(b"Run complete")
        except Exception as e:
            print(f"Run failed: {e}")
            self.send_response(500); self.end_headers()
            self.wfile.write(str(e).encode())

    def log_message(self, *args): pass


def cli():
    parser = argparse.ArgumentParser(description="Rankovi GEO Engine v2")
    parser.add_argument("--brand-id",       type=int,  help="Run a specific brand")
    parser.add_argument("--all",            action="store_true", help="Run all active brands")
    parser.add_argument("--passes",         type=int,  help="Passes per prompt (overrides .env)")
    parser.add_argument("--competitor-id",  type=int,  help="Run prompts targeting competitor brand")
    parser.add_argument("--onboard",        action="store_true", help="Run onboarding research pipeline")
    parser.add_argument("--seed",           type=str,  help="Seed keyword for onboarding")
    parser.add_argument("--competitor-domain", type=str, help="Competitor domain for onboarding")
    parser.add_argument("--max-prompts",    type=int,  default=50)
    args = parser.parse_args()

    if args.onboard:
        if not args.brand_id or not args.seed:
            print("--onboard requires --brand-id and --seed")
            sys.exit(1)
        from research.onboarding import run_onboarding
        run_onboarding(
            brand_id=args.brand_id,
            seed_keyword=args.seed,
            competitor_domain=args.competitor_domain,
            max_total_prompts=args.max_prompts,
        )
    elif args.brand_id:
        run_brand(args.brand_id, triggered_by="manual",
                  passes=args.passes,
                  target_brand_id=args.competitor_id)
    elif args.all:
        run_all_brands(triggered_by="manual", passes=args.passes)
    else:
        parser.print_help()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 0))
    if port:
        print(f"Starting HTTP server on port {port}")
        HTTPServer(("0.0.0.0", port), RunHandler).serve_forever()
    else:
        cli()
