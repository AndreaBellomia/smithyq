# 🔨 SmithyQ

**Forge your tasks with type safety**

SmithyQ is a high-performance async task worker library for Rust that brings the power of compile-time type safety to distributed task processing.

[![Crates.io](https://img.shields.io/crates/v/smithyq.svg)](https://crates.io/crates/smithyq)
[![Documentation](https://docs.rs/smithyq/badge.svg)](https://docs.rs/smithyq)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![Build Status](https://github.com/AndreaBellomia/smithyq/workflows/CI/badge.svg)](https://github.com/AndreaBellomia/smithyq/actions)

## 🚀 Features

- **🔒 Type-Safe**: Compile-time verification of task payloads and return types
- **⚡ High Performance**: Built on Tokio for maximum async performance
- **🎯 Auto-Registration**: Automatic task registration with macros
- **🔄 Multiple Backends**: In-memory, Redis, and PostgreSQL queue support
- **📊 Observability**: Built-in metrics, tracing, and monitoring
- **🛡️ Fault Tolerant**: Graceful shutdown, retry logic, and error handling
- **📅 Scheduling**: Support for delayed and recurring tasks

## 📦 Installation

Add SmithyQ to your `Cargo.toml`:

```toml
[dependencies]
smithyq = "0.1"

# For Redis queue backend
smithyq = { version = "0.1", features = ["redis-queue"] }

# For PostgreSQL queue backend  
smithyq = { version = "0.1", features = ["postgres-queue"] }

# For all features
smithyq = { version = "0.1", features = ["full"] }


## 🛣️ Roadmap - Upcoming Features

### 📋 Queue Backends
- [ ] **Redis Queue**: Complete Redis backend implementation for distributed queue management
- [ ] **RabbitMQ Queue**: Support for RabbitMQ as backend for robust and scalable messaging
- [ ] **AWS SQS**: Integration with Amazon Simple Queue Service for cloud-native deployments

### 🔧 Middleware & Extensibility
- [ ] **Tower Integration**: Integration with Tower framework for composable middleware
  - Logging middleware
  - Rate limiting
  - Authentication/Authorization
  - Request/Response transformation
  - Circuit breaker pattern

### 🎯 Advanced Features
- [ ] **Dead Letter Queue**: Automatic handling of failed tasks
- [ ] **Task Prioritization**: Priority system for task execution
- [ ] **Batch Processing**: Process tasks in batches for improved efficiency
- [ ] **Health Checks**: Endpoints for worker status monitoring

### 📊 Monitoring & Observability
- [ ] **Prometheus Metrics**: Export metrics in Prometheus format
- [ ] **OpenTelemetry**: Full support for distributed tracing
- [ ] **Dashboard**: Web interface for real-time monitoring
