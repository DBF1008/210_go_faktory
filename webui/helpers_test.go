package webui

import (
	"fmt"
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

type testJob struct {
	name     string
	job      *client.Job
	expected string
}

func makeActiveJob(jobType string) *client.Job {
	var payload = fmt.Appendf(nil, `
{
  "jid": "bb2a34025e7ce72a064d10d6",
  "queue": "default",
  "jobtype": "ActiveJob::QueueAdapters::FaktoryAdapter::JobWrapper",
  "args": [
    {
      "arguments": [
        1, 2, 3
      ],
      "enqueued_at": "2023-12-20T20:36:09.423631911Z",
      "exception_executions": {},
      "executions": 0,
      "job_class": "%[1]s",
      "job_id": "b24332fc-49bb-4128-8c4c-230c47abea91",
      "locale": "en",
      "priority": null,
      "provider_job_id": "bb2a34025e7ce72a064d10d6",
      "queue_name": "default",
      "scheduled_at": null,
      "timezone": "Pacific Time (US & Canada)"
    }
  ],
  "created_at": "2023-12-20T20:36:09.423723003Z",
  "enqueued_at": "2023-12-20T20:36:09.423725007Z",
  "retry": 3,
  "custom": {
    "wrapped": "%[1]s"
  }
}
`, jobType)
	job := &client.Job{}
	err := util.JsonUnmarshal(payload, job)
	if err != nil {
		panic(err)
	}
	return job
}

func makeActionMailerJob(jobType string, mailerClass string, mailerMethod string) *client.Job {
	var payload = fmt.Appendf(nil, `
{
  "jid": "60259247a836111c54b5ddf7",
  "queue": "default",
  "jobtype": "ActiveJob::QueueAdapters::FaktoryAdapter::JobWrapper",
  "args": [
    {
      "arguments": [
        "%[2]s",
        "%[3]s",
        "deliver_now",
        {
          "_aj_ruby2_keywords": [
            "args"
          ],
          "args": [
            {
              "_aj_globalid": "gid://app/User/1"
            }
          ]
        }
      ],
      "enqueued_at": "2023-12-20T21:35:27.281042097Z",
      "exception_executions": {},
      "executions": 0,
      "job_class": "%[1]s",
      "job_id": "0667e00a-61e4-4936-af9f-8ba494c060a0",
      "locale": "en",
      "priority": null,
      "provider_job_id": "60259247a836111c54b5ddf7",
      "queue_name": "default",
      "scheduled_at": null,
      "timezone": "Pacific Time (US & Canada)"
    }
  ],
  "created_at": "2023-12-20T21:35:27.281126796Z",
  "enqueued_at": "2023-12-20T21:35:27.281128369Z",
  "retry": 3,
  "custom": {
    "wrapped": "%[1]s"
  }
}
`, jobType, mailerClass, mailerMethod)
	job := &client.Job{}
	err := util.JsonUnmarshal(payload, job)
	if err != nil {
		panic(err)
	}
	return job
}

var testCases = []testJob{
	{
		name:     "simple",
		job:      client.NewJob("foo_job", 1),
		expected: "foo_job",
	},
	{
		name:     "wrapped",
		job:      makeActiveJob("FooJob"),
		expected: "FooJob",
	},
	{
		name:     "Rails 5.x mailer",
		job:      makeActionMailerJob("ActionMailer::DeliveryJob", "UserMailer", "welcome"),
		expected: "UserMailer#welcome",
	},
	{
		name:     "Rails 6.x mailer",
		job:      makeActionMailerJob("ActionMailer::MailDeliveryJob", "UserMailer", "welcome"),
		expected: "UserMailer#welcome",
	},
}

func TestActiveJobUnwrapping(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := displayJobType(tc.job)
			if tc.expected != result {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}

func TestFormatLatency(t *testing.T) {
	cases := []struct {
		name     string
		secs     float64
		expected string
	}{
		{"zero (empty queue)", 0, "-"},
		{"sub-second", 0.3, "0.3s"},
		{"few seconds", 5.7, "5.7s"},
		{"just under a minute", 59.9, "59.9s"},
		{"one minute", 60.0, "1m"},
		{"minutes and seconds", 125.0, "2m 5s"},
		{"exact minutes", 180.0, "3m"},
		{"one hour", 3600.0, "1h"},
		{"hours and minutes", 5400.0, "1h 30m"},
		{"large value", 7260.0, "2h 1m"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, formatLatency(tc.secs))
		})
	}
}

func TestCategoryForLatency(t *testing.T) {
	assert.Equal(t, "muted", categoryForLatency(0))
	assert.Equal(t, "success", categoryForLatency(0.5))
	assert.Equal(t, "success", categoryForLatency(9.9))
	assert.Equal(t, "warning", categoryForLatency(10))
	assert.Equal(t, "warning", categoryForLatency(59.9))
	assert.Equal(t, "danger", categoryForLatency(60))
	assert.Equal(t, "danger", categoryForLatency(3600))
}
