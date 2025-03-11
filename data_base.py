import mysql.connector

# Database Configuration
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "123123"
DB_NAME = "pose_estimation_db"


def create_database():
    """Creates the database and tables if they don't exist."""
    try:
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD)
        cursor = conn.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        conn.commit()
        conn.close()

        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()

        # Create Participant table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Participant (
                participant_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255)
            )
        """)

        # Create Session table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Session (
                session_id INT AUTO_INCREMENT PRIMARY KEY,
                participant_id INT,
                start_time DATETIME,
                end_time DATETIME,
                FOREIGN KEY (participant_id) REFERENCES Participant(participant_id)
            )
        """)

        # Create Activity table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Activity (
                activity_id INT AUTO_INCREMENT PRIMARY KEY,
                session_id INT,
                activity_type VARCHAR(255),
                start_time DATETIME,
                end_time DATETIME,
                duration INT,
                FOREIGN KEY (session_id) REFERENCES Session(session_id)
            )
        """)

        # Create VideoRecording table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS VideoRecording (
                recording_id INT AUTO_INCREMENT PRIMARY KEY,
                session_id INT,
                activity_type VARCHAR(255),
                file_path VARCHAR(255),
                fps FLOAT,
                width INT,
                height INT,
                frame_count INT,
                FOREIGN KEY (session_id) REFERENCES Session(session_id)
            )
        """)

        # Create PoseEstimationDataset table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PoseEstimationDataset (
                pose_id INT AUTO_INCREMENT PRIMARY KEY,
                recording_id INT,
                frame INT,
                person_id INT,
                timestamp FLOAT,
                joint_00_x FLOAT, joint_00_y FLOAT, joint_00_v FLOAT,
                joint_01_x FLOAT, joint_01_y FLOAT, joint_01_v FLOAT,
                joint_02_x FLOAT, joint_02_y FLOAT, joint_02_v FLOAT,
                joint_03_x FLOAT, joint_03_y FLOAT, joint_03_v FLOAT,
                joint_04_x FLOAT, joint_04_y FLOAT, joint_04_v FLOAT,
                joint_05_x FLOAT, joint_05_y FLOAT, joint_05_v FLOAT,
                joint_06_x FLOAT, joint_06_y FLOAT, joint_06_v FLOAT,
                joint_07_x FLOAT, joint_07_y FLOAT, joint_07_v FLOAT,
                joint_08_x FLOAT, joint_08_y FLOAT, joint_08_v FLOAT,
                joint_09_x FLOAT, joint_09_y FLOAT, joint_09_v FLOAT,
                joint_10_x FLOAT, joint_10_y FLOAT, joint_10_v FLOAT,
                joint_11_x FLOAT, joint_11_y FLOAT, joint_11_v FLOAT,
                joint_12_x FLOAT, joint_12_y FLOAT, joint_12_v FLOAT,
                joint_13_x FLOAT, joint_13_y FLOAT, joint_13_v FLOAT,
                joint_14_x FLOAT, joint_14_y FLOAT, joint_14_v FLOAT,
                joint_15_x FLOAT, joint_15_y FLOAT, joint_15_v FLOAT,
                joint_16_x FLOAT, joint_16_y FLOAT, joint_16_v FLOAT,
                FOREIGN KEY (recording_id) REFERENCES VideoRecording(recording_id)
            )
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Database and tables created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating database/tables: {err}")

def insert_participant(name):
    """Inserts a new participant into the database."""
    try:
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()
        sql = "INSERT INTO Participant (name) VALUES (%s)"
        val = (name,)
        cursor.execute(sql, val)
        conn.commit()
        participant_id = cursor.lastrowid
        cursor.close()
        conn.close()
        return participant_id
    except mysql.connector.Error as err:
        print(f"Error inserting participant: {err}")
        return None

def insert_session(participant_id, start_time, end_time):
    """Inserts a new session into the database."""
    try:
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()
        sql = "INSERT INTO Session (participant_id, start_time, end_time) VALUES (%s, %s, %s)"
        val = (participant_id, start_time, end_time)
        cursor.execute(sql, val)
        conn.commit()
        session_id = cursor.lastrowid
        cursor.close()
        conn.close()
        return session_id
    except mysql.connector.Error as err:
        print(f"Error inserting session: {err}")
        return None
    
def update_session(participant_id, end_time):
    """Update sessin end_time in database."""
    try:
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()
        sql = "UPDATE Session SET end_time = %s WHERE participant_id = %s"
        val = (end_time, participant_id)
        cursor.execute(sql, val)
        conn.commit()
        session_id = cursor.lastrowid
        cursor.close()
        conn.close()
        return session_id
    except mysql.connector.Error as err:
        print(f"Error updating session: {err}")
        return None

def insert_activity(session_id, activity_type, start_time, end_time, duration):
    """Inserts a new activity into the database."""
    try:
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()
        sql = "INSERT INTO Activity (session_id, activity_type, start_time, end_time, duration) VALUES (%s, %s, %s, %s, %s)"
        val = (session_id, activity_type, start_time, end_time, duration)
        cursor.execute(sql, val)
        conn.commit()
        activity_id = cursor.lastrowid
        cursor.close()
        conn.close()
        return activity_id
    except mysql.connector.Error as err:
        print(f"Error inserting activity: {err}")
        return None

def get_activity_duration(session_id, activity_type):
    """Get activity duration from database."""
    try:
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()
        sql = "SELECT * FROM Activity WHERE session_id = %s AND activity_type = %s"
        val = (session_id, activity_type)
        cursor.execute(sql, val)
        result = cursor.fetchone()  # Now myresult holds the dat
        cursor.close()
        conn.close()
        return result[-1]
    except mysql.connector.Error as err:
        print(f"Error grting duration: {err}")
        return None

def insert_video_recording(session_id, activity_type, file_path, fps, width, height, frame_count):
    """Inserts video recording metadata into the database."""
    try:
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()
        sql = "INSERT INTO VideoRecording (session_id, activity_type, file_path, fps, width, height, frame_count) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (session_id, activity_type, file_path, fps, width, height, frame_count)
        cursor.execute(sql, val)
        conn.commit()
        recording_id = cursor.lastrowid
        cursor.close()
        conn.close()
        return recording_id
    except mysql.connector.Error as err:
        print(f"Error inserting video recording: {err}")
        return None

def insert_pose_data(recording_id, frame, person_id, timestamp, keypoints):
    """Inserts pose estimation data into the database."""
    try:
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()

        # Flatten the keypoints list and prepare the SQL query
        keypoint_values = []
        for kp in keypoints:
            if kp is None:
                keypoint_values.extend([None, None, None])  # Handle missing keypoint
            else:
                keypoint_values.extend(list(kp.values()))

        sql = """
            INSERT INTO PoseEstimationDataset (
                recording_id, frame, person_id, timestamp,
                joint_00_x, joint_00_y, joint_00_v,
                joint_01_x, joint_01_y, joint_01_v,
                joint_02_x, joint_02_y, joint_02_v,
                joint_03_x, joint_03_y, joint_03_v,
                joint_04_x, joint_04_y, joint_04_v,
                joint_05_x, joint_05_y, joint_05_v,
                joint_06_x, joint_06_y, joint_06_v,
                joint_07_x, joint_07_y, joint_07_v,
                joint_08_x, joint_08_y, joint_08_v,
                joint_09_x, joint_09_y, joint_09_v,
                joint_10_x, joint_10_y, joint_10_v,
                joint_11_x, joint_11_y, joint_11_v,
                joint_12_x, joint_12_y, joint_12_v,
                joint_13_x, joint_13_y, joint_13_v,
                joint_14_x, joint_14_y, joint_14_v,
                joint_15_x, joint_15_y, joint_15_v,
                joint_16_x, joint_16_y, joint_16_v
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        val = (recording_id, frame, person_id, timestamp, *keypoint_values)  # Unpack keypoint_values
        cursor.execute(sql, val)
        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error inserting pose data: {err}")
        if conn and conn.is_connected():
            conn.rollback()
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()