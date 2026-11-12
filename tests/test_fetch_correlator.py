from unittest.mock import patch
import socket
import urllib.error

from rca.analysis.correlator import fetch_correlated_logs


@patch("rca.analysis.correlator.SplunkClient")
def test_fetch_correlated_logs_calls_client(mock_client_class, mock_config):
    """Test fetch_correlated_logs calls SplunkClient correctly."""
    # Setup mock client instance
    mock_client = mock_client_class.return_value
    mock_client.query_ocp_namespace.return_value = []
    mock_client.query_by_guid.return_value = []

    job_context = {
        "job_id": "123",
        "guid": "guid-1",
        "namespace": "ns-1",
        "time_window": {
            "started": "2023-10-27T10:00:00Z",
            "finished": "2023-10-27T10:10:00Z",
        },
    }

    results = fetch_correlated_logs(mock_config, job_context)

    # Verify client instantiation
    mock_client_class.assert_called_once_with(mock_config)

    # Verify namespace query
    mock_client.query_ocp_namespace.assert_called()
    args, kwargs = mock_client.query_ocp_namespace.call_args
    assert args[0] == "ns-1"
    assert kwargs["earliest"] == "2023-10-27T10:00:00Z"
    assert kwargs["latest"] == "2023-10-27T10:10:00Z"

    # Verify structure of results
    assert results["job_id"] == "123"
    assert results["guid"] == "guid-1"
    assert "ocp_logs" in results
    assert "pods_found" in results


@patch("rca.analysis.correlator.SplunkClient")
def test_fetch_correlated_logs_skips_remaining_queries_when_splunk_is_unavailable(
    mock_client_class, mock_config
):
    mock_client = mock_client_class.return_value
    mock_client.query_ocp_namespace.side_effect = urllib.error.URLError(
        socket.gaierror(-2, "Name or service not known")
    )

    job_context = {
        "job_id": "123",
        "guid": "guid-1",
        "namespace": "ns-1",
        "time_window": {
            "started": "2023-10-27T10:00:00Z",
            "finished": "2023-10-27T10:10:00Z",
        },
    }

    results = fetch_correlated_logs(mock_config, job_context)

    assert results["skipped"] is True
    assert "Name or service not known" in results["reason"]
    assert results["errors"] == []
    mock_client.query_by_guid.assert_not_called()
