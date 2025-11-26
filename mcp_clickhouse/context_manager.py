def get_user_context(request_context: dict[str, Any]) -> Optional[UserContext]:
    return request_context.get("user_name"), request_context.get("company_id")
