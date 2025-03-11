import cv2
import datetime
import json
from data_base import *
from ultralytics import YOLO

PARTICIPANT_NAME = "Ed"

# YOLO Pose Model
MODEL_PATH = "yolov8n-pose.pt"
model = YOLO(MODEL_PATH)

CALIBRATION_ACTIVITY_TIME = 10
MAIN_ACTIVITY_TIME = 30

def record_session(participant_id, output_file="session.mp4"):
    cap = cv2.VideoCapture(0)

    if not cap.isOpened():
        print("Error: Could not open camera.")
        return

    fps = int(cap.get(cv2.CAP_PROP_FPS))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_file, fourcc, fps, (width, height))

    start_time = datetime.datetime.now()
    frame_count = 0
    end_time = start_time # update later
    session_id = insert_session(participant_id, start_time, end_time)

    ACTIVITY_TYPE = "Calibration"

    # Calibration activity phase
    print(f"{ACTIVITY_TYPE} activity: Stand in A-Pose for {CALIBRATION_ACTIVITY_TIME} seconds...")
    calibration_end_time = start_time + datetime.timedelta(seconds=CALIBRATION_ACTIVITY_TIME)
    while datetime.datetime.now() < calibration_end_time:
        ret, frame = cap.read()
        if not ret:
            break
        out.write(frame)
        frame_count += 1
        cv2.imshow('Recording', frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    # Store metadata in MySQL
    insert_activity(session_id, ACTIVITY_TYPE, start_time, end_time, int(duration.seconds))
    insert_video_recording(session_id, ACTIVITY_TYPE, output_file, fps, width, height, frame_count)

    start_time = datetime.datetime.now()
    frame_count = 0

    ACTIVITY_TYPE = "Main"

    print(f"Recording {ACTIVITY_TYPE} activity: Stand or move for {MAIN_ACTIVITY_TIME} seconds...")
    main_end_time = start_time + datetime.timedelta(seconds=MAIN_ACTIVITY_TIME)
    while datetime.datetime.now() < main_end_time:
        ret, frame = cap.read()
        if not ret:
            break
        out.write(frame)
        frame_count += 1
        cv2.imshow('Recording', frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    # Store metadata in MySQL
    insert_activity(session_id, ACTIVITY_TYPE, start_time, end_time, int(duration.seconds))
    # Store recording_id only for Main activity
    recording_id = insert_video_recording(session_id, ACTIVITY_TYPE, output_file, fps, width, height, frame_count)

    # update Session with end time
    update_session(session_id, end_time)

    cap.release()
    out.release()
    cv2.destroyAllWindows()
    return session_id, recording_id

def process_recording(session_id, recording_id, video_path="session.mp4", activity_type="Main"):

    cap = cv2.VideoCapture(video_path)

    if not cap.isOpened():
        print("Error: Could not open video.")
        return

    fps = int(cap.get(cv2.CAP_PROP_FPS))
    frame_count = 0

    # Get duration time from the database.
    activity_duration = get_activity_duration(session_id, activity_type)
    recording_frames = activity_duration * fps

    while frame_count < recording_frames:
        ret, frame = cap.read()
        if not ret:
            break

        results = model.predict(frame, verbose=False)
        for result in results:
            if result.keypoints is not None:
                keypoints = result.keypoints.xy.cpu().numpy().tolist()
                visibility = result.keypoints.conf.cpu().numpy().tolist()
                for person_id, (person_keypoints, person_visibility) in enumerate(zip(keypoints, visibility)):
                    joint_data = [{"x": kp[0], "y": kp[1], "v": vis} for kp, vis in zip(person_keypoints, person_visibility)]
                    insert_pose_data(recording_id, frame_count, person_id, cap.get(cv2.CAP_PROP_POS_MSEC), joint_data)
        frame_count += 1

    cap.release()


if __name__ == "__main__":

    create_database()
    participant_id = insert_participant(PARTICIPANT_NAME)
    session_id, recording_id = record_session(participant_id)
    if recording_id:
        print(f"Processing Recording for Session ID: {recording_id}")
        process_recording(session_id, recording_id)
        print(f"Processing done!")
