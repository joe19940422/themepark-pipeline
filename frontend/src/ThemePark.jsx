import React, { useState, useEffect } from 'react';
import './ThemePark.css';
import { getAttractionImage } from './attractionImages.js';
import { attractionNames } from './attractionNames.js';

function ThemeParkDashboard() {
  const [attractions, setAttractions] = useState([]);
  const [currentTime, setCurrentTime] = useState(new Date());
  const [isConnected, setIsConnected] = useState(false);

  useEffect(() => {
    const timer = setInterval(() => setCurrentTime(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    let eventSource;
    
    const connectSSE = () => {
      eventSource = new EventSource('http://localhost:8002/api/analytics/stream');

      eventSource.onopen = () => {
        console.log('SSE Connected');
        setIsConnected(true);
      };
      
      eventSource.onerror = (error) => {
        console.error('SSE Error:', error);
        setIsConnected(false);
        eventSource.close();
        // Reconnect after 5 seconds
        setTimeout(connectSSE, 5000);
      };

      eventSource.onmessage = (event) => {
        console.log('SSE Message received:', event.data);
        try {
          const message = JSON.parse(event.data);
          if (message.type === 'analytics' && message.data) {
            console.log('Updating attractions:', message.data.length, 'items');
            setAttractions(message.data.sort((a, b) => (b.avg_waittime || 0) - (a.avg_waittime || 0)));
          }
        } catch (e) {
          console.error('Parse error:', e);
        }
      };
    };

    connectSSE();

    // Fetch initial data
    fetch('http://localhost:8002/api/attractions')
      .then(r => r.json())
      .then(data => {
        if (data.attractions) {
          console.log('Initial attractions:', data.attractions.length);
          setAttractions(data.attractions.sort((a, b) => (b.avg_waittime || 0) - (a.avg_waittime || 0)));
        }
      })
      .catch(err => console.error('Fetch error:', err));

    return () => {
      if (eventSource) {
        eventSource.close();
      }
    };
  }, []);

  const formatTime = (date) => {
    return date.toLocaleTimeString('en-US', { 
      timeZone: 'Asia/Shanghai',
      hour12: false, 
      hour: '2-digit', 
      minute: '2-digit', 
      second: '2-digit'
    });
  };

  const getWaitTimeColor = (waitTime) => {
    if (waitTime < 20) return '#10b981';
    if (waitTime < 40) return '#f59e0b';
    return '#ef4444';
  };

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-content">
          <div className="digital-clock">
            {formatTime(currentTime)}
            <div style={{ fontSize: '0.7rem', opacity: 0.8, marginTop: '0.2rem' }}>Shanghai Time</div>
          </div>
          <div className={`status-indicator ${isConnected ? 'connected' : 'disconnected'}`}>
            {isConnected ? '● LIVE' : '○ OFFLINE'}
          </div>
        </div>
        <h1>🎢 Shanghai Disney Theme Park Wait Times</h1>
        <p>Real-time attraction analytics powered by Apache Flink </p>
        <p style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '0.5rem' }}>
          Created by Fei
          <a href="https://www.linkedin.com/in/pengfeichiao/" target="_blank" rel="noopener noreferrer" style={{ display: 'inline-flex', alignItems: 'center' }}>
            <img src="https://upload.wikimedia.org/wikipedia/commons/c/ca/LinkedIn_logo_initials.png" alt="LinkedIn" style={{ height: '24px', width: '24px' }} />
          </a>
        </p>
        <div style={{ display: 'flex', gap: '2rem', justifyContent: 'center', alignItems: 'center', marginTop: '1rem', flexWrap: 'wrap' }}>
          <img src="https://kafka.apache.org/logos/kafka_logo--simple.png" alt="Apache Kafka" style={{ height: '40px', filter: 'brightness(0) invert(1)' }} />
          <img src="https://flink.apache.org/img/logo/png/1000/flink_squirrel_1000.png" alt="Apache Flink" style={{ height: '50px' }} />
          <img src="https://upload.wikimedia.org/wikipedia/commons/b/ba/Confluent_Logo.png" alt="Confluent" style={{ height: '35px' }} />
        </div>
      </header>

      <main className="app-main">
        <div className="attractions-grid">
          {attractions.length === 0 ? (
            <div className="empty-state">
              <div className="empty-icon">⏳</div>
              <h3>Waiting for data...</h3>
              <p>Analytics will appear here once Flink processes the stream</p>
            </div>
          ) : (
            attractions.map((attr, index) => (
              <div key={attr.entityId} className="attraction-card">
                <div className="card-image">
                  <img 
                    src={getAttractionImage(attr.name)} 
                    alt={attr.name}
                    onError={(e) => e.target.src = 'https://images.unsplash.com/photo-1594818379496-da1e345b0ded?w=400&h=300&fit=crop'}
                  />
                  <div className="rank-badge">#{index + 1}</div>
                </div>
                <div className="card-header">
                  <span className="attraction-name">
                    {attr.name || attr.entityId}
                    {attractionNames[attr.name] && (
                      <div style={{ fontSize: '0.9rem', marginTop: '0.3rem', opacity: 0.9 }}>
                        {attractionNames[attr.name]}
                      </div>
                    )}
                  </span>
                </div>
                <div className="card-body">
                  <div 
                    className="wait-time"
                    style={{ color: getWaitTimeColor(attr.avg_waittime) }}
                  >
                    {attr.avg_waittime === -1 ? 'CLOSED' : Math.round(attr.avg_waittime || 0)}
                    {attr.avg_waittime !== -1 && <span className="unit">min</span>}
                  </div>
                  <div className="label">Average Wait Time</div>
                </div>
                <div className="card-footer">
                  <div className="progress-bar">
                    <div 
                      className="progress-fill"
                      style={{ 
                        width: `${Math.min(100, (attr.avg_waittime / 60) * 100)}%`,
                        backgroundColor: getWaitTimeColor(attr.avg_waittime)
                      }}
                    />
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </main>
    </div>
  );
}

export default ThemeParkDashboard;
