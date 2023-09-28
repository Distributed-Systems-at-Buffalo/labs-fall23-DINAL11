package mapreduce

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	// Create a channel to receive task completion signals.
	taskCompleted := make(chan bool)

	// Loop through all the tasks and assign them to available workers.
	for taskNumber := 0; taskNumber < ntasks; taskNumber++ {
		go func(taskNum int) {
			for {
				worker := <-mr.registerChannel // Wait for a registered worker.

				// Construct the DoTaskArgs for the task.
				args := DoTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[taskNum],
					Phase:         phase,
					TaskNumber:    taskNum,
					NumOtherPhase: nios,
				}

				// Send the task to the worker.
				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if ok {
					// If the task completed successfully, notify the channel.
					taskCompleted <- true

					// Return the worker to the pool.
					go func() {
						mr.registerChannel <- worker
					}()
					break
				}
			}
		}(taskNumber)
	}

	// Wait for all tasks to complete.
	for i := 0; i < ntasks; i++ {
		<-taskCompleted
	}

	close(taskCompleted) // Close the taskCompleted channel when done.

	debug("Schedule: %v phase done\n", phase)
}
