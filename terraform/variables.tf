variable "dd_api_key" {
  description = "Datadog API Key"
  type        = string
  sensitive   = true
}

variable "dd_app_key" {
  description = "Datadog Application Key"
  type        = string
  sensitive   = true
}

variable "dd_api_url" {
  description = "Datadog API URL"
  type        = string
  default     = "https://api.datadoghq.com/"
}

variable "notification_channel" {
  description = "Notification target for monitors (e.g., @slack-channel, @email)"
  type        = string
  default     = "@pedro.schawirin@datadoghq.com"
}
