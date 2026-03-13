def test_admin_page_renders(client):
    response = client.get("/admin")
    assert response.status_code == 200
    assert "LovelyChaos Admin Console" in response.text


def test_onboarding_page_renders(client):
    response = client.get("/onboarding")
    assert response.status_code == 200
    assert "LovelyChaos Onboarding" in response.text
    assert "/admin/schools/search" in response.text


def test_inbound_activity_page_renders(client):
    response = client.get("/admin/activity")
    assert response.status_code == 200
    assert "Inbound Activity" in response.text


def test_admin_settings_and_children_flow(client):
    update = client.put(
        "/admin/settings",
        json={
            "daily_summary_enabled": True,
            "weekly_digest_enabled": True,
        },
    )
    assert update.status_code == 200

    child = client.post("/admin/children", json={"name": "Ava", "school_name": "PS 101", "grade": "2"})
    assert child.status_code == 200

    children = client.get("/admin/children")
    assert children.status_code == 200
    assert any(c["name"] == "Ava" for c in children.json())


def test_school_search_endpoint(client, monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(
        main_module,
        "search_gta_schools",
        lambda query, limit=8: [
            {
                "school_name": "Frankland Community School Junior",
                "board_name": "Toronto DSB",
                "city": "Toronto",
                "province": "Ontario",
                "postal_code": "M4J4N1",
                "street": "816 Logan Ave",
                "school_level": "Elementary",
                "school_language": "English",
                "school_type": "Public",
                "timezone": "America/Toronto",
                "source": "test",
            }
        ],
    )

    response = client.get("/admin/schools/search", params={"q": "frank"})
    assert response.status_code == 200
    body = response.json()
    assert body["results"][0]["school_name"] == "Frankland Community School Junior"


def test_child_create_updates_household_timezone(client, monkeypatch):
    import app.main as main_module
    from app.services.school_directory import SchoolResolution

    monkeypatch.setattr(
        main_module,
        "resolve_school_timezone",
        lambda school_name: SchoolResolution(
            school_name="Frankland Community School Junior",
            timezone="America/Toronto",
            city="Toronto",
            province="Ontario",
            board_name="Toronto DSB",
            postal_code="M4J4N1",
            source="test",
            matched_from_directory=True,
        ),
    )

    child = client.post("/admin/children", json={"name": "Ava", "school_name": "Frankland", "grade": "2"})
    assert child.status_code == 200
    payload = child.json()
    assert payload["resolved_timezone"] == "America/Toronto"
    assert payload["school_name"] == "Frankland Community School Junior"

    profile = client.get("/admin/profile")
    assert profile.status_code == 200
    assert profile.json()["timezone"] == "America/Toronto"


def test_onboarding_profile_flow(client):
    save = client.post(
        "/onboarding/profile/form",
        data={
            "admin_email": "newadmin@example.com",
            "secondary_admin_email": "secondparent@example.com",
            "admin_phone": "+15558889999",
            "timezone_value": "America/Toronto",
            "spouse_phone": "+15557770000",
            "spouse_notifications_enabled": "on",
        },
    )
    assert save.status_code == 200

    profile = client.get("/admin/profile")
    assert profile.status_code == 200
    body = profile.json()
    assert body["admin_email"] == "newadmin@example.com"
    assert body["secondary_admin_email"] == "secondparent@example.com"
    assert body["timezone"] == "America/Toronto"
