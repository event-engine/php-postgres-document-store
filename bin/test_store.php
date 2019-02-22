<?php
/**
 * This file is part of the event-engine/php-postgres-document-store.
 * (c) 2019 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

use EventEngine\DocumentStore\Filter;
use EventEngine\DocumentStore\OrderBy;
use EventEngine\DocumentStore;

require_once __DIR__ .'/../vendor/autoload.php';

$dsn = getenv('PDO_DSN');
$usr = getenv('PDO_USER');
$pwd = getenv('PDO_PWD');

$pdo = new \PDO($dsn, $usr, $pwd);
$pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

$pdo->prepare("DROP TABLE IF EXISTS em_ds_docs")->execute();
$pdo->prepare("DROP TABLE IF EXISTS em_ds_animals")->execute();
$pdo->prepare("DROP TABLE IF EXISTS em_ds_test")->execute();

$documentStore = new \EventEngine\DocumentStore\Postgres\PostgresDocumentStore($pdo);

$documentStore->addCollection('test');
$documentStore->addCollection('animals',
    DocumentStore\FieldIndex::forField('name', DocumentStore\FieldIndex::SORT_DESC, true),
    DocumentStore\MultiFieldIndex::forFields([
        DocumentStore\FieldIndex::forField('character.friendly'),
        DocumentStore\FieldIndex::forField('pet')
    ])
);

$collections = $documentStore->listCollections();

foreach ($collections as $col) {
    echo "Collection: $col\n";
}

$collections = $documentStore->filterCollectionsByPrefix('do');

foreach ($collections as $col) {
    echo "Filtered Collection: $col\n";
}

echo "Has test: " . ($documentStore->hasCollection('test')? 'YES' : 'NO') . "\n";
echo "Has dogs: " . ($documentStore->hasCollection('dogs')? 'YES' : 'NO') . "\n";

$documentStore->dropCollection('test');
echo "Table test dropped\n";
echo "Has test: " . ($documentStore->hasCollection('test')? 'YES' : 'NO') . "\n";

$docId1 = '0e2c39b1-0778-4e92-a06b-0fa1c6ae9c5d';
$docId2 = '0e2ec294-df90-4bd7-a934-74747752f0bb';
$docId3 = '5bd4fdee-bfe2-4e1d-a4bf-b27e57fa1686';

$documentStore->addDoc('animals', $docId1, [
    'animal' => 'dog',
    'name' => 'Jack',
    'age' => 5,
    'character' => [
        'friendly' => 10,
        'wild' => 6,
        'docile' => 8
    ]
]);

$jack = $documentStore->getDoc('animals', $docId1);

echo "Jack: " . json_encode($jack) . "\n";

try {
    $documentStore->upsertDoc('animals', $docId2, [
        'animal' => 'cat',
        'name' => 'Jack',
        'age' => 5,
        'character' => [
            'friendly' => 3,
            'wild' => 7,
            'docile' => 2
        ]
    ]);
} catch (PDOException $exception) {
    if($exception->errorInfo[0] === "23505") {
        echo "Cannot add Jack twice due to unqiue index on name.\n";
    } else {
        throw $exception;
    }
}

$documentStore->upsertDoc('animals', $docId2, [
    'animal' => 'cat',
    'name' => 'Tiger',
    'age' => 5,
    'character' => [
        'friendly' => 3,
        'wild' => 7,
        'docile' => 2
    ]
]);

$documentStore->upsertDoc('animals', $docId2, [
    'age' => 3
]);

$tiger = $documentStore->getDoc('animals', $docId2);

echo "Tiger: " . json_encode($tiger) . "\n";

$documentStore->addDoc('animals', $docId3, [
    'animal' => 'cat',
    'name' => 'Gini',
    'age' => 5,
    'character' => [
        'friendly' => 8,
        'wild' => 3,
        'docile' => 4
    ]
]);

$cats = $documentStore->filterDocs('animals', new Filter\EqFilter('animal', 'cat'));

foreach ($cats as $cat) {
    echo "Cat: " . json_encode($cat) . "\n";
}

$tiNames = $documentStore->filterDocs(
    'animals',
    new Filter\LikeFilter('name', 'Ti%')
);

foreach ($tiNames as $tiName) {
    echo "Animal name starting with Ti: " . json_encode($tiName) . "\n";
}

$documentStore->updateMany('animals', new Filter\GteFilter('character.friendly', 5), ['pet' => true]);

$pets = $documentStore->filterDocs('animals', new Filter\EqFilter('pet', true));

foreach ($pets as $pet) {
    echo "Pet: " . json_encode($pet) . "\n";
}

$superFriendlyPets = $documentStore->filterDocs(
    'animals',
    new Filter\AndFilter(
        new Filter\EqFilter('pet', true),
        new Filter\EqFilter('character.friendly', 10)
    )
);

foreach ($superFriendlyPets as $pet) {
    echo "Super friendly pet: " . json_encode($pet) . "\n";
}

$documentStore->updateDoc('animals', $docId1, ['color' => ['black', 'white']]);
$documentStore->updateDoc('animals', $docId2, ['color' => ['grey', 'black', 'white']]);
$documentStore->updateDoc('animals', $docId3, ['color' => ['brown', 'red', 'white']]);

$petsWithBlack = $documentStore->filterDocs(
    'animals',
    new Filter\AndFilter(
        new Filter\EqFilter('pet', true),
        new Filter\InArrayFilter('color', 'black')
    )
);

foreach ($petsWithBlack as $pet) {
    echo "Pet with black: " . json_encode($pet) . "\n";
}

$noPets = $documentStore->filterDocs(
    'animals',
    new Filter\NotFilter(new Filter\ExistsFilter('pet'))
);

foreach ($noPets as $pet) {
    echo "Not a pet: " . json_encode($pet) . "\n";
}

$oldestAnimals = $documentStore->filterDocs(
    'animals',
    new Filter\AnyFilter(),
    null,
    2,
    OrderBy\AndOrder::by(
        OrderBy\Desc::byProp('age'),
        OrderBy\Asc::byProp('name')
    )
);

foreach ($oldestAnimals as $animal) {
    echo "Old animal: " . json_encode($animal) . "\n";
}

$sortedAnimals = $documentStore->filterDocs(
    'animals',
    new Filter\AnyFilter(),
    1,
    2,
    OrderBy\Asc::byProp('name')
);

foreach ($sortedAnimals as $animal) {
    echo "Sorted animal: " . json_encode($animal) . "\n";
}


$documentStore->deleteDoc('animals', $docId2);
echo "Deleted doc with id $docId2\n";

$animalsWithWhite = $documentStore->filterDocs(
    'animals',
    New Filter\InArrayFilter('color', 'white')
);

foreach ($animalsWithWhite as $animal) {
    echo "Animal with color white: " . json_encode($animal) . "\n";
}

$documentStore->deleteMany('animals', new Filter\InArrayFilter('color', 'white'));

echo "Deleted animals with color white\n";

$animalsWithWhite = $documentStore->filterDocs(
    'animals',
    New Filter\InArrayFilter('color', 'white')
);

foreach ($animalsWithWhite as $animal) {
    echo "Animal with color white: " . json_encode($animal) . "\n";
}
