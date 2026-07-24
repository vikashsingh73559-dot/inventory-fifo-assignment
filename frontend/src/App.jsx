import { useEffect, useState } from 'react';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:5000';

function App() {
  const [isLoggedIn, setIsLoggedIn] = useState(localStorage.getItem('isLoggedIn') === 'true');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [stock, setStock] = useState([]);
  const [ledger, setLedger] = useState([]);
  const [loading, setLoading] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);

  const tableStyle = { width: '100%', borderCollapse: 'collapse', marginTop: '10px', boxShadow: '0 2px 10px rgba(0,0,0,0.1)' };
  const thStyle = { border: '1px solid #ddd', padding: '12px', backgroundColor: '#f8f9fa', textAlign: 'left', fontWeight: 'bold' };
  const tdStyle = { border: '1px solid #ddd', padding: '12px', verticalAlign: 'top' };

  const handleLogin = (event) => {
    event.preventDefault();
    if (username === 'admin' && password === 'admin123') {
      setIsLoggedIn(true);
      localStorage.setItem('isLoggedIn', 'true');
      return;
    }
    alert('Wrong ID or password');
  };

  const handleLogout = () => {
    setIsLoggedIn(false);
    localStorage.removeItem('isLoggedIn');
  };

  const fetchData = async () => {
    try {
      const [stockResponse, ledgerResponse] = await Promise.all([
        fetch(`${API_BASE_URL}/api/stock`),
        fetch(`${API_BASE_URL}/api/ledger`),
      ]);

      setStock(await stockResponse.json());
      setLedger(await ledgerResponse.json());
      setLastUpdated(new Date());
    } catch (error) {
      console.error('Data refresh failed:', error);
    }
  };

  useEffect(() => {
    if (!isLoggedIn) return undefined;

    const initialRefreshTimer = setTimeout(fetchData, 0);
    const refreshTimer = setInterval(fetchData, 5000);
    return () => {
      clearTimeout(initialRefreshTimer);
      clearInterval(refreshTimer);
    };
  }, [isLoggedIn]);

  const simulateEvent = async () => {
    setLoading(true);
    const isPurchase = Math.random() > 0.4;
    const eventData = {
      product_id: 'PRD001',
      event_type: isPurchase ? 'purchase' : 'sale',
      quantity: Math.floor(Math.random() * 20) + 1,
      timestamp: new Date().toISOString(),
    };

    if (isPurchase) {
      eventData.unit_price = Number((Math.random() * 50 + 50).toFixed(2));
    }

    try {
      await fetch(`${API_BASE_URL}/api/inventory`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(eventData),
      });

      setTimeout(fetchData, 1500);
    } catch (error) {
      console.error('Simulation failed:', error);
    } finally {
      setLoading(false);
    }
  };

  if (!isLoggedIn) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '100vh', backgroundColor: '#f4f7f6' }}>
        <form onSubmit={handleLogin} style={{ padding: '40px', backgroundColor: 'white', borderRadius: '8px', boxShadow: '0 4px 12px rgba(0,0,0,0.1)', textAlign: 'center', width: '300px' }}>
          <h2>Login</h2>
          <input
            type="text"
            placeholder="User ID (admin)"
            value={username}
            onChange={(event) => setUsername(event.target.value)}
            style={{ display: 'block', width: '90%', margin: '15px auto', padding: '10px', borderRadius: '4px', border: '1px solid #ccc' }}
            required
          />
          <input
            type="password"
            placeholder="Password (admin123)"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            style={{ display: 'block', width: '90%', margin: '15px auto', padding: '10px', borderRadius: '4px', border: '1px solid #ccc' }}
            required
          />
          <button type="submit" style={{ width: '95%', padding: '10px', backgroundColor: '#007bff', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer', fontSize: '16px', fontWeight: 'bold' }}>
            Login
          </button>
        </form>
      </div>
    );
  }

  return (
    <div style={{ padding: '30px', fontFamily: 'Arial, sans-serif', maxWidth: '1100px', margin: 'auto', backgroundColor: '#fff' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
        <div>
          <h1 style={{ color: '#333', marginBottom: '4px' }}>Inventory Dashboard (FIFO)</h1>
          <small style={{ color: '#666' }}>
            {lastUpdated ? `Live refresh every 5s. Last updated: ${lastUpdated.toLocaleTimeString()}` : 'Loading live data...'}
          </small>
        </div>
        <div>
          <button
            onClick={simulateEvent}
            disabled={loading}
            style={{ padding: '10px 20px', backgroundColor: '#28a745', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer', fontSize: '14px', fontWeight: 'bold', marginRight: '10px' }}
          >
            {loading ? 'Processing...' : 'Simulate Event'}
          </button>
          <button
            onClick={handleLogout}
            style={{ padding: '10px 20px', backgroundColor: '#6c757d', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer', fontSize: '14px', fontWeight: 'bold' }}
          >
            Logout
          </button>
        </div>
      </div>

      <h2 style={{ color: '#007bff', marginTop: '30px' }}>Product Stock Overview</h2>
      <table style={tableStyle}>
        <thead>
          <tr>
            <th style={thStyle}>Product ID</th>
            <th style={thStyle}>Current Quantity</th>
            <th style={thStyle}>Total Inventory Cost</th>
            <th style={thStyle}>Average Cost per Unit</th>
          </tr>
        </thead>
        <tbody>
          {stock.map((item) => (
            <tr key={item.product_id} style={{ backgroundColor: '#fdfdfd' }}>
              <td style={tdStyle}><b>{item.product_id}</b></td>
              <td style={tdStyle}>{item.current_quantity}</td>
              <td style={tdStyle}>{Number(item.total_cost).toFixed(2)}</td>
              <td style={tdStyle}>{Number(item.average_cost).toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h2 style={{ color: '#dc3545', marginTop: '40px' }}>Transaction Ledger</h2>
      <table style={tableStyle}>
        <thead>
          <tr>
            <th style={thStyle}>Date & Time</th>
            <th style={thStyle}>Type</th>
            <th style={thStyle}>Product ID</th>
            <th style={thStyle}>Quantity</th>
            <th style={thStyle}>Total Value / FIFO Cost</th>
            <th style={thStyle}>FIFO Breakdown</th>
          </tr>
        </thead>
        <tbody>
          {ledger.map((row, index) => {
            const breakdown = Array.isArray(row.fifo_breakdown) ? row.fifo_breakdown : [];

            return (
              <tr key={`${row.event_type}-${row.sale_id || row.timestamp}-${index}`} style={{ backgroundColor: row.event_type === 'purchase' ? '#e6ffe6' : '#ffe6e6' }}>
                <td style={tdStyle}>{new Date(row.timestamp).toLocaleString()}</td>
                <td style={tdStyle}>
                  <span style={{ padding: '4px 8px', borderRadius: '4px', backgroundColor: row.event_type === 'purchase' ? '#28a745' : '#dc3545', color: 'white', fontSize: '12px', fontWeight: 'bold' }}>
                    {row.event_type.toUpperCase()}
                  </span>
                </td>
                <td style={tdStyle}>{row.product_id}</td>
                <td style={tdStyle}>{row.quantity}</td>
                <td style={tdStyle}>{Number(row.total_cost).toFixed(2)}</td>
                <td style={tdStyle}>
                  {row.event_type === 'sale'
                    ? breakdown.map((item) => `Batch ${item.batch_id}: ${item.quantity} x ${Number(item.unit_price).toFixed(2)} = ${Number(item.cost).toFixed(2)}`).join(', ')
                    : '-'}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default App;
