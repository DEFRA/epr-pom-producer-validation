using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EPR.ProducerContentValidation.Application.Models;
using EPR.ProducerContentValidation.Application.Options;
using EPR.ProducerContentValidation.Application.Services;
using EPR.ProducerContentValidation.Application.Services.Interfaces;
using EPR.ProducerContentValidation.Application.Utils.Mongo;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Driver;
using Moq;

namespace EPR.ProducerContentValidation.Application.UnitTests.Services;

[TestClass]
public class IssueCountServiceTests
{
    private const string MockKey = "mock-key";
    private const int MaxIssuesToProcess = 1000;
    private const int IssuesToProcess = 100;

    private Mock<IMongoDbClientFactory> _clientFactoryMock;
    private Mock<IMongoCollection<IssueCountDocument>> _collectionMock;
    private Mock<IOptions<ValidationOptions>> _validationOptionsMock;
    private IIssueCountService _serviceUnderTest;

    public IssueCountServiceTests()
    {
        _clientFactoryMock = new Mock<IMongoDbClientFactory>();
        _collectionMock = new Mock<IMongoCollection<IssueCountDocument>>();
        _validationOptionsMock = new Mock<IOptions<ValidationOptions>>();
    }

    [TestInitialize]
    public void TestInitialize()
    {
        _validationOptionsMock.Setup(x => x.Value)
            .Returns(new ValidationOptions { MaxIssuesToProcess = MaxIssuesToProcess });

        _clientFactoryMock
            .Setup(x => x.GetCollection<IssueCountDocument>(It.IsAny<string>()))
            .Returns(_collectionMock.Object);

        SetupStoredCount(IssuesToProcess);

        _serviceUnderTest = new IssueCountService(_clientFactoryMock.Object, _validationOptionsMock.Object);
    }

    [TestMethod]
    public async Task IncrementIssueCountAsync_WhenCalled_PerformsUpsertIncrement()
    {
        // Arrange
        const int mockCount = 5;
        _collectionMock
            .Setup(x => x.UpdateOneAsync(
                It.IsAny<FilterDefinition<IssueCountDocument>>(),
                It.IsAny<UpdateDefinition<IssueCountDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((UpdateResult)null);

        // Act
        await _serviceUnderTest.IncrementIssueCountAsync(MockKey, mockCount);

        // Assert
        _collectionMock.Verify(
            x => x.UpdateOneAsync(
                It.IsAny<FilterDefinition<IssueCountDocument>>(),
                It.IsAny<UpdateDefinition<IssueCountDocument>>(),
                It.Is<UpdateOptions>(o => o.IsUpsert),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [TestMethod]
    public async Task GetRemainingIssueCapacityAsync_WhenStoredCountBelowMax_ReturnsTheDifference()
    {
        // Act
        var result = await _serviceUnderTest.GetRemainingIssueCapacityAsync(MockKey);

        // Assert
        result.Should().Be(MaxIssuesToProcess - IssuesToProcess);
    }

    [TestMethod]
    public async Task GetRemainingIssueCapacityAsync_WhenNoStoredDocument_ReturnsMaxIssuesToProcess()
    {
        // Arrange
        SetupStoredCount(null);

        // Act
        var result = await _serviceUnderTest.GetRemainingIssueCapacityAsync(MockKey);

        // Assert
        result.Should().Be(MaxIssuesToProcess);
    }

    [TestMethod]
    public async Task GetRemainingIssueCapacityAsync_WhenStoredCountExceedsMax_ReturnsZero()
    {
        // Arrange
        SetupStoredCount(MaxIssuesToProcess + 1);

        // Act
        var result = await _serviceUnderTest.GetRemainingIssueCapacityAsync(MockKey);

        // Assert
        result.Should().Be(0);
    }

    private void SetupStoredCount(int? storedCount)
    {
        var documents = storedCount.HasValue
            ? new List<IssueCountDocument> { new() { Id = MockKey, Count = storedCount.Value } }
            : new List<IssueCountDocument>();

        var cursorMock = new Mock<IAsyncCursor<IssueCountDocument>>();
        cursorMock.Setup(x => x.Current).Returns(documents);
        cursorMock
            .SetupSequence(x => x.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);

        _collectionMock
            .Setup(x => x.FindAsync(
                It.IsAny<FilterDefinition<IssueCountDocument>>(),
                It.IsAny<FindOptions<IssueCountDocument, IssueCountDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursorMock.Object);
    }
}
