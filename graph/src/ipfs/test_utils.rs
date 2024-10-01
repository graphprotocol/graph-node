use reqwest::multipart;
use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct IpfsAddFile {
    path: String,
    content: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct IpfsAddResponse {
    pub name: String,
    pub hash: String,
}

impl From<Vec<u8>> for IpfsAddFile {
    fn from(content: Vec<u8>) -> Self {
        Self {
            path: Default::default(),
            content: content.into(),
        }
    }
}

impl<T, U> From<(T, U)> for IpfsAddFile
where
    T: Into<String>,
    U: Into<Vec<u8>>,
{
    fn from((path, content): (T, U)) -> Self {
        Self {
            path: path.into(),
            content: content.into(),
        }
    }
}

pub async fn add_files_to_local_ipfs_node_for_testing<T, U>(
    files: T,
) -> anyhow::Result<Vec<IpfsAddResponse>>
where
    T: IntoIterator<Item = U>,
    U: Into<IpfsAddFile>,
{
    let mut form = multipart::Form::new();

    for file in files.into_iter() {
        let file = file.into();
        let part = multipart::Part::bytes(file.content).file_name(file.path);

        form = form.part("path", part);
    }

    let resp = reqwest::Client::new()
        .post("http://127.0.0.1:5001/api/v0/add")
        .multipart(form)
        .send()
        .await?
        .text()
        .await?;

    let mut output = Vec::new();

    for line in resp.lines() {
        let line = line.trim();

        if line.is_empty() {
            continue;
        }

        output.push(serde_json::from_str::<IpfsAddResponse>(line)?);
    }

    Ok(output)
}
