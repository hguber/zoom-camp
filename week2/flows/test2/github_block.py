from prefect.filesystems import GitHub

github_block = GitHub.load("zoom-github")
github_block.get_directory('week2/flows/test2')
github_block.save("dev")
