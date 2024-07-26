# Configuring the Google Cloud provider
provider "google" {
  project = "data-pipeline-qa"
  region  = "us-west1"
}

# Creating a Pub/Sub topic
resource "google_pubsub_topic" "example_topic" {
  name = "example-bigquery-topic"
}

# Creating a Pub/Sub subscription
resource "google_pubsub_subscription" "example_subscription" {
  name  = "example-bigquery-subscription"
  topic = google_pubsub_topic.example_topic.id

  # Optional: Configure push endpoint or ack deadline
  ack_deadline_seconds = 20
}
