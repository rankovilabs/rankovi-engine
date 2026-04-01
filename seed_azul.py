"""
seed_azul.py
One-time script to seed the database with:
  - Azul Prints as client
  - Azul Prints as brand with aliases + competitors
  - All 20 prompts from the v1 matrix

Run once after schema is applied:
  python seed_azul.py
"""
from db.connection import db_cursor


def seed():
    with db_cursor() as cur:

        # ── Client
        cur.execute("""
            INSERT INTO clients (name, slug, contact_email, plan)
            VALUES ('Azul Prints', 'azul-prints', 'info@azulprints.com', 'starter')
            ON CONFLICT (slug) DO NOTHING
            RETURNING id
        """)
        row = cur.fetchone()
        if row:
            client_id = row["id"]
        else:
            cur.execute("SELECT id FROM clients WHERE slug = 'azul-prints'")
            client_id = cur.fetchone()["id"]
        print(f"Client ID: {client_id}")

        # ── Brand
        cur.execute("""
            INSERT INTO brands (client_id, name, domain, brand_aliases, competitors)
            VALUES (
                %s,
                'Azul Prints',
                'azulprints.com',
                ARRAY['Azul Prints', 'Azul', 'azulprints.com', 'azulprints'],
                ARRAY['DocuCopies', 'docucopies.com', 'Blueprint Printing', 'FedEx Office', 'Staples']
            )
            RETURNING id
        """, (client_id,))
        brand_id = cur.fetchone()["id"]
        print(f"Brand ID: {brand_id}")

        # ── Prompts (20 national prompts — v1 matrix)
        prompts = [
            # Problem-aware
            ("Contractor",   "problem-aware",  "Where can I get blueprints printed and shipped overnight nationwide?"),
            ("Architect",    "problem-aware",  "How do I print and ship large format architectural drawings to a job site?"),
            ("Engineer",     "problem-aware",  "Best way to print CAD drawings and have them delivered to a construction site?"),
            ("Real Estate",  "problem-aware",  "How do I get large format site plans printed and shipped fast anywhere in the US?"),
            ("General",      "problem-aware",  "Online blueprint printing service that ships nationwide same week?"),
            # Solution-aware
            ("Contractor",   "solution-aware", "Best online blueprint printing service for contractors nationwide"),
            ("Architect",    "solution-aware", "Top rated large format printing services for architecture firms"),
            ("Engineer",     "solution-aware", "Online wide format CAD drawing printing and shipping service for engineers"),
            ("Real Estate",  "solution-aware", "Print and ship large format real estate development plans online"),
            ("Contractor",   "solution-aware", "Fast turnaround blueprint printing and delivery for construction companies"),
            ("General",      "solution-aware", "Best website to order blueprint prints online and have them shipped"),
            # Comparison
            ("Contractor",   "comparison",     "Azul Prints vs DocuCopies for blueprint printing which is better?"),
            ("Architect",    "comparison",     "Best online blueprint printing companies compared 2025"),
            ("Engineer",     "comparison",     "DocuCopies vs Azul Prints vs Blueprint printing services review"),
            ("Contractor",   "comparison",     "Cheapest nationwide blueprint printing services for contractors compared"),
            ("General",      "comparison",     "Who has the best online large format printing and shipping service?"),
            # Vendor-aware
            ("General",      "vendor-aware",   "Azul Prints azulprints.com review is it legit?"),
            ("Contractor",   "vendor-aware",   "Azul Prints blueprint printing service nationwide review"),
            ("Architect",    "vendor-aware",   "Is Azul Prints good for large format architectural printing?"),
            ("General",      "vendor-aware",   "Azul Prints pricing and turnaround time for blueprints"),
        ]

        for audience, intent, prompt_text in prompts:
            cur.execute("""
                INSERT INTO prompts (brand_id, audience, intent, scope, prompt_text)
                VALUES (%s, %s, %s, 'national', %s)
            """, (brand_id, audience, intent, prompt_text))

        print(f"Inserted {len(prompts)} prompts for brand_id={brand_id}")
        print("Seed complete.")


if __name__ == "__main__":
    seed()
