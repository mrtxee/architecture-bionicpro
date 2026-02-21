import React, { useState } from "react";
import { useKeycloak } from "@react-keycloak/web";

interface Report {
  username: string;
  email: string;
  date_of_birth: string;
  timestamp: string;
  sensor_value: number;
}

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reportData, setReportData] = useState<Report[] | null>(null);
  const [userInfo, setUserInfo] = useState<{
    username: string;
    date_of_birth: string;
  } | null>(null);

  const downloadReport = async () => {
    if (!keycloak?.token) {
      setError("Not authenticated");
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
        headers: {
          Authorization: `Bearer ${keycloak.token}`,
        },
      });

      if (!response.ok) {
        const errText = await response.text();
        throw new Error(`Error ${response.status}: ${errText}`);
      }

      const data = await response.json();
      setReportData(data.reports);

      // Берём username и дату рождения из первой строки, если есть данные
      if (data.reports && data.reports.length > 0) {
        setUserInfo({
          username: data.reports[0].username,
          date_of_birth: data.reports[0].date_of_birth,
        });
      } else {
        setUserInfo(null);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "An error occurred");
      setReportData(null);
      setUserInfo(null);
    } finally {
      setLoading(false);
    }
  };

  const logout = () => {
    keycloak.logout({ redirectUri: window.location.origin });
  };

  if (!initialized) {
    return <div>Loading...</div>;
  }

  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => keycloak.login()}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100 p-4">
      <div className="p-8 bg-white rounded-lg shadow-md w-full max-w-3xl">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold">Usage Reports</h1>
          <button
            onClick={logout}
            className="px-4 py-2 bg-red-500 text-white rounded hover:bg-red-600"
          >
            Logout
          </button>
        </div>

        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? "opacity-50 cursor-not-allowed" : ""
          }`}
        >
          {loading ? "Loading Report..." : "Load Report"}
        </button>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}

        {/* Информация о пользователе */}
        {userInfo && (
          <div className="mt-6 p-4 bg-gray-50 rounded shadow-sm">
            <p>
              <strong>Username:</strong> {userInfo.username}
            </p>
            <p>
              <strong>Date of Birth:</strong> {userInfo.date_of_birth}
            </p>
          </div>
        )}

        {/* Таблица с динамическими данными */}
        {reportData && reportData.length > 0 && (
          <div className="mt-6 overflow-x-auto">
            <table className="min-w-full border border-gray-200">
              <thead className="bg-gray-100">
                <tr>
                  <th className="border px-4 py-2">Timestamp</th>
                  <th className="border px-4 py-2">Sensor Value</th>
                </tr>
              </thead>
              <tbody>
                {reportData.map((row, idx) => (
                  <tr key={idx} className="even:bg-gray-50">
                    <td className="border px-4 py-2">{row.timestamp}</td>
                    <td className="border px-4 py-2">{row.sensor_value}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {reportData && reportData.length === 0 && (
          <div className="mt-4 text-gray-600">No reports available.</div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;
