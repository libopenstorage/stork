#!groovy

pipeline {
	agent {
		label "porx-builder"
	}

	stages {
	    stage("Build Torpedo") {
            steps {
                build(job: "${CBT_TORPEDO_BUILD_JOB_NAME}", parameters: [string(name: "GIT_BRANCH", value: GIT_BRANCH)])
            }
        }
		stage("Run Torpedo CBT") {
			steps {
				build(job: "${CBT_TORPEDO_TEST_JOB_NAME}", parameters: [string(name: "GIT_BRANCH", value: GIT_BRANCH)])
			}
		}
	}
}
