import pytest
from requests import Response

from src.repositories.cvr_api_repository import CvrRepository

############################################## pytest fixtures ##############################################
@pytest.fixture
def cvr_repo():
    return CvrRepository(username='test_user', password='test_password')

@pytest.fixture
def valid_scroll_response(mocker):
    mock_respone = mocker.Mock(spec=Response)
    mock_respone.json.return_value = {"_scroll_id": "12345"}
    return mock_respone

@pytest.fixture
def invalid_scroll_response(mocker):
    mock_respone = mocker.Mock(spec=Response)
    mock_respone.json.return_value = {"no_scroll": "12345"}
    return mock_respone

@pytest.fixture
def mock_session(mocker):
    session = mocker.MagicMock()
    mock_response = mocker.Mock(spec=Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "some_data"}
    
    session.get.return_value = mock_response
    return session
###################################################################################################################


def test_get_scroll_id_success(cvr_repo, valid_scroll_response):
    scroll_id = cvr_repo.get_scroll_id(cvr_data=valid_scroll_response)
    assert scroll_id == "12345"

def test_get_scroll_id_fail(cvr_repo, invalid_scroll_response):

    with pytest.raises(AttributeError, match="Invalid response from cvr virk endpoint!: 'dict' object has no attribute 'json'"):
        cvr_repo.get_scroll_id(cvr_data={})
    
    with pytest.raises(KeyError, match="_scroll_id not found in response."):
        cvr_repo.get_scroll_id(cvr_data=invalid_scroll_response)

def test_extract_cvr_virksomhed_data_success(mock_session):
    repo = CvrRepository(username="test_user", password="test_pass", session=mock_session)
    
    query = {"query": {"match_all": {}}}
    response = repo.extract_cvr_virksomhed_data(query)
    
    assert response.status_code == 200
    assert response.json() == {"data": "some_data"}

