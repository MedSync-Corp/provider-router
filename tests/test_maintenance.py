from __future__ import annotations

import re
import unittest
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENTRY_PAGES = {
    "action-items.html",
    "credentials.html",
    "dashboard.html",
    "index.html",
    "login.html",
    "pipeline.html",
    "providers.html",
    "roster.html",
    "settings.html",
}


class MaintenanceHTMLInspector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tags: list[tuple[str, dict[str, str | None]]] = []
        self.text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append((tag, dict(attrs)))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.text.append(data.strip())


class MaintenancePageTests(unittest.TestCase):
    def test_every_tracked_entry_page_is_the_same_maintenance_surface(self) -> None:
        actual_pages = {path.name for path in ROOT.glob("*.html")}
        self.assertEqual(actual_pages, ENTRY_PAGES)

        canonical = (ROOT / "index.html").read_text(encoding="utf-8")
        for page_name in sorted(ENTRY_PAGES):
            with self.subTest(page=page_name):
                self.assertEqual((ROOT / page_name).read_text(encoding="utf-8"), canonical)

    def test_maintenance_pages_have_no_legacy_code_or_network_entry_points(self) -> None:
        prohibited_fragments = {
            "<script",
            "<form",
            "<input",
            "<button",
            "<select",
            "<textarea",
            "fetch(",
            "xmlhttprequest",
            "apirequest",
            "dataclient",
            "auth.js",
            "env.js",
            "api.medsyncorp.com",
            "supabase",
            "directshifts",
            "medsync connect",
            "ehr_login",
            "password",
            "copy to clipboard",
            "reveal",
            "routing",
            "assignment",
            "availability",
            "capacity",
            "billable",
            "billability",
            "http://",
            "https://",
        }

        for page_name in sorted(ENTRY_PAGES):
            with self.subTest(page=page_name):
                source = (ROOT / page_name).read_text(encoding="utf-8").lower()
                for fragment in prohibited_fragments:
                    self.assertNotIn(fragment, source)

    def test_accessibility_and_safe_resource_contract(self) -> None:
        source = (ROOT / "index.html").read_text(encoding="utf-8")
        inspector = MaintenanceHTMLInspector()
        inspector.feed(source)

        tags = inspector.tags
        tag_names = [tag for tag, _ in tags]
        html_attributes = next(attrs for tag, attrs in tags if tag == "html")
        main_attributes = next(attrs for tag, attrs in tags if tag == "main")

        self.assertEqual(html_attributes.get("lang"), "en")
        self.assertEqual(main_attributes.get("id"), "main-content")
        self.assertEqual(main_attributes.get("tabindex"), "-1")
        self.assertEqual(tag_names.count("main"), 1)
        self.assertEqual(tag_names.count("h1"), 1)
        self.assertEqual(tag_names.count("h2"), 1)
        self.assertNotIn("script", tag_names)
        self.assertNotIn("form", tag_names)
        self.assertNotIn("button", tag_names)

        viewport = [attrs for tag, attrs in tags if tag == "meta" and attrs.get("name") == "viewport"]
        self.assertEqual(len(viewport), 1)
        self.assertIn("width=device-width", viewport[0].get("content", ""))

        content_security_policies = [
            attrs
            for tag, attrs in tags
            if tag == "meta" and attrs.get("http-equiv") == "Content-Security-Policy"
        ]
        self.assertEqual(len(content_security_policies), 1)
        self.assertIn("default-src 'none'", content_security_policies[0].get("content", ""))
        self.assertIn("form-action 'none'", content_security_policies[0].get("content", ""))

        links = [attrs for tag, attrs in tags if tag == "a"]
        self.assertEqual(links, [{"class": "skip-link", "href": "#main-content"}])

        stylesheets = [attrs.get("href") for tag, attrs in tags if tag == "link"]
        self.assertEqual(stylesheets, ["maintenance.css"])
        images = [(attrs.get("src"), attrs.get("alt")) for tag, attrs in tags if tag == "img"]
        self.assertEqual(images, [("medsync-logo.png", "MedSync")])

        visible_text = " ".join(inspector.text)
        self.assertIn("Provider Router is temporarily unavailable.", visible_text)
        self.assertIn("Provider Network view in the MedSync CRM", visible_text)
        self.assertIn("Contact your MedSync administrator.", visible_text)

        for _, attributes in tags:
            self.assertFalse(any(name.lower().startswith("on") for name in attributes))

    def test_focus_responsive_and_contrast_smoke_checks(self) -> None:
        css = (ROOT / "maintenance.css").read_text(encoding="utf-8")
        self.assertIn(":focus-visible", css)
        self.assertIn("@media (max-width: 40rem)", css)
        self.assertIn("@media (prefers-reduced-motion: reduce)", css)
        self.assertIn("clamp(", css)
        self.assertIn("min-width: 20rem", css)

        colors = dict(re.findall(r"--([\w-]+):\s*(#[0-9a-fA-F]{6})", css))
        for foreground, background in [
            ("ink", "paper"),
            ("body", "paper"),
            ("muted", "paper"),
            ("status-ink", "status-wash"),
            ("brand-blue", "paper"),
        ]:
            ratio = contrast_ratio(colors[foreground], colors[background])
            self.assertGreaterEqual(
                ratio,
                4.5,
                f"expected WCAG AA contrast for {foreground} on {background}, got {ratio:.2f}",
            )


def contrast_ratio(first: str, second: str) -> float:
    lighter, darker = sorted((relative_luminance(first), relative_luminance(second)), reverse=True)
    return (lighter + 0.05) / (darker + 0.05)


def relative_luminance(color: str) -> float:
    channels = [int(color[index : index + 2], 16) / 255 for index in (1, 3, 5)]
    linear = [channel / 12.92 if channel <= 0.04045 else ((channel + 0.055) / 1.055) ** 2.4 for channel in channels]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


if __name__ == "__main__":
    unittest.main()
